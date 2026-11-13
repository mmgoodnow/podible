import Foundation

struct PodibleClient {
    private let baseURL: URL
    private let token: String

    init(baseURL: URL, token: String) {
        self.baseURL = baseURL
        self.token = token
    }

    func browse() async throws -> [MediaTitle] {
        async let library: Library = call("catalog.available", LibraryQuery(query: nil, limit: nil))
        async let discovery: Discovery = call("catalog.discovery", Empty())
        let (published, trending) = try await (library, discovery)
        return unique(libraryTitles(published)
            + trending.movies.items.map { title($0.tmdbId, $0.title, $0.year.flatMap(Int.init(exactly:)), nil, $0.posterUrl, kind: .movie, state: .requestable) }
            + trending.tv.items.map { title($0.tmdbId, $0.title, $0.year.flatMap(Int.init(exactly:)), nil, $0.posterUrl, kind: .show, state: .requestable) })
    }

    func search(_ query: String) async throws -> [MediaTitle] {
        let query = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return [] }
        guard query.utf16.count <= 200 else { throw ClientError.invalidQuery }

        async let library: Library = call("catalog.available", LibraryQuery(query: query, limit: nil))
        async let movies = searchMovies(query)
        async let tv: TVSearch = call("tv.search", SearchQuery(query: query))
        let (published, films, shows) = try await (library, movies, tv)
        let results = unique(libraryTitles(published)
            + films.results.map { title($0.tmdbId, $0.title, $0.year, $0.overview, $0.posterUrl, kind: .movie, state: films.canRequest ? .requestable : .unavailable) }
            + shows.results.map { title($0.tmdbId, $0.title, $0.year, $0.overview, $0.posterUrl, kind: .show, state: .requestable) })
        if results.isEmpty && (films.failed || (shows.configured && shows.unavailableReason != nil)) {
            throw ClientError.catalogUnavailable
        }
        return results
    }

    func detail(_ item: MediaTitle) async throws -> (MediaTitle, [MediaRequestOption], TitleAcquisitionProgress?) {
        switch item.kind {
        case .movie:
            guard let tmdbId = item.tmdbID, tmdbId > 0 else { throw ClientError.missingIdentity }
            async let details: MovieDetail = call("movies.get", MovieID(tmdbId: tmdbId))
            async let status: MovieStatus = call("movies.status", Empty())
            let (movie, capabilities) = try await (details, status)
            let active = movie.requests.filter { $0.state != "withdrawn" }
            let state: RequestState = movie.availableInLibrary || active.contains(where: \.available) ? .available
                : !active.isEmpty ? .requested
                : capabilities.acquisitionConfigured ? .requestable : .unavailable
            let progress = active.first { $0.stage == "downloading" } ?? active.first(where: \.available) ?? active.first
            let info = movie.movie
            return (title(info.tmdbId, info.title, info.year, info.overview, info.posterUrl, kind: .movie, state: state), [], progress?.progress)
        case .show:
            return try await tvDetail(item)
        }
    }

    func request(_ item: MediaTitle, selection: Set<MediaRequestOption.ID>) async throws {
        switch item.kind {
        case .movie:
            guard let tmdbId = item.tmdbID, tmdbId > 0 else { throw ClientError.missingIdentity }
            let _: MovieRequestReceipt = try await call("movies.request", MovieID(tmdbId: tmdbId))
        case .show:
            let seasons = selection.compactMap(\.seasonNumber).sorted()
            let specials = selection.compactMap(\.specialKey).sorted()
            guard !selection.isEmpty, seasons.count <= 500, specials.count <= 500,
                  seasons.allSatisfy({ (1...9_007_199_254_740_991).contains($0) }) else {
                throw ClientError.invalidSelection
            }
            let tmdbId: Int?
            if let knownID = item.tmdbID {
                tmdbId = knownID
            } else {
                tmdbId = try await libraryEntry(for: item)?.entry.tmdbId
            }
            guard let tmdbId, tmdbId > 0 else { throw ClientError.missingIdentity }
            let _: TVRequestReceipt = try await call(
                "tv.request", TVRequestSelection(tmdbId: tmdbId, seasonNumbers: seasons.isEmpty ? nil : seasons,
                                                 specialEpisodeKeys: specials.isEmpty ? nil : specials)
            )
        }
    }

    func requests() async throws -> [MediaTitle] {
        async let movies: MovieRequests = call("requests.list", Empty())
        async let tv: TVRequests = call("tv.requestCards", Empty())
        let (films, shows) = try await (movies, tv)
        return unique(films.requests.filter { $0.state != "withdrawn" && !$0.available }.map {
            let item = $0.movie
            return title(item.tmdbId, item.title, item.year, item.overview, item.posterUrl, kind: .movie, state: .requested)
        } + shows.requests.filter { $0.intentionallyAbsent || $0.availableEpisodes < $0.requestedEpisodes }.map {
            libraryTitle($0.entry.key, $0.entry.title, $0.entry.year, $0.entry.posterUrl, entryID: $0.entryId,
                         state: $0.intentionallyAbsent ? .unavailable : .requested)
        })
    }

    func beginPairing() async throws -> DeviceLoginGrant {
        let grant: DeviceLoginGrant = try await call("auth.beginDeviceLogin", DeviceLoginInput(qrOnly: true), requiresSession: false)
        guard grant.id.range(of: "^[A-Za-z0-9_-]{32}$", options: .regularExpression) != nil,
              grant.verifier.range(of: "^[A-Za-z0-9_-]{43}$", options: .regularExpression) != nil,
              grant.authorizePath.range(of: #"^/auth/device\?id=\#(grant.id)&approval=[A-Za-z0-9_-]{43}$"#,
                                        options: .regularExpression) != nil,
              (1...300).contains(grant.expiresInSeconds) else { throw ClientError.invalidResponse }
        return grant
    }

    func exchangePairing(_ grant: DeviceLoginGrant) async throws -> DeviceLoginExchange {
        try await call("auth.exchangeDeviceLogin", PairingExchangeInput(id: grant.id, verifier: grant.verifier),
                       requiresSession: false)
    }

    func signOut() async throws {
        let _: LogoutReceipt = try await call("auth.logout", Empty())
    }

    private func searchMovies(_ query: String) async throws -> (results: [MovieSearch.ResultsValue], canRequest: Bool, failed: Bool) {
        let status: MovieStatus = try await call("movies.status", Empty())
        guard status.catalogConfigured else { return ([], false, false) }
        do {
            let response: MovieSearch = try await call("movies.search", SearchQuery(query: query))
            return (response.results, status.acquisitionConfigured, false)
        } catch ClientError.rpc(-32000) {
            return ([], false, true)
        }
    }

    private func tvDetail(_ item: MediaTitle) async throws -> (MediaTitle, [MediaRequestOption], TitleAcquisitionProgress?) {
        var localID = entryID(for: item)
        var local = try await libraryEntry(for: item)
        let tmdbId = local?.entry.tmdbId ?? item.tmdbID
        var remote: TVDetail?
        if let tmdbId, tmdbId > 0 {
            let response: TVDetail = try await call("tv.details", MovieID(tmdbId: tmdbId))
            // A local work is not interchangeable with a different entry that shares a TMDB reference.
            if entryID(for: item) == nil || response.showId == entryID(for: item) {
                remote = response
            }
            if local == nil, let showId = response.showId {
                local = try await call("catalog.tvEntry", EntryID(entryId: showId))
                localID = showId
            }
        }

        let coverage = remote?.seasons ?? []
        let specials = remote?.specials ?? []
        let seasons = remote?.series?.seasons.filter { $0.seasonNumber != 0 || specials.isEmpty }.map { season in
            let current = coverage.first { $0.number == Double(season.seasonNumber) }
            let available = current.flatMap { Int(exactly: $0.available) } ?? 0
            let ready = season.episodeCount > 0 && available == season.episodeCount
            let requested = current?.requested == true
            let canRequest = season.seasonNumber > 0 && season.requestable && season.episodeCount > 0 && !ready && !requested
            let summary: String
            if ready { summary = "Ready in Plex" }
            else if requested { summary = "Already requested" }
            else if !season.requestable { summary = season.unavailableReason ?? "Season unavailable" }
            else { summary = "\(available) of \(season.episodeCount) episodes ready" }
            return MediaRequestOption(id: .season(season.seasonNumber), name: season.title, summary: summary, requestable: canRequest)
        } ?? local?.progress.seasons.map { season in
            MediaRequestOption(id: .season(season.seasonNumber), name: season.seasonNumber == 0 ? "Specials" : "Season \(season.seasonNumber)",
                        summary: "\(season.available) of \(season.total) episodes ready", requestable: false)
        } ?? []
        let options = seasons + specials.map { special in
            let name = "Special \(special.episodeNumber)" + (special.title.isEmpty ? "" : ": \(special.title)")
            let summary = special.available ? "Ready in Plex" : special.requested ? "Already requested" : special.airDate ?? ""
            return MediaRequestOption(id: .special(special.episodeKey), name: name, summary: summary,
                                      requestable: !special.available && !special.requested)
        }

        let state: RequestState
        if (local?.availableEpisodes ?? 0) > 0 || coverage.contains(where: { $0.available > 0 }) { state = .available }
        else if options.contains(where: \.requestable) { state = .requestable }
        else if local?.requests.isEmpty == false || coverage.contains(where: \.requested) || specials.contains(where: \.requested) { state = .requested }
        else { state = .unavailable }

        let series = remote?.series
        return (MediaTitle(id: item.id, title: series?.title ?? local?.entry.title ?? item.title,
                           subtitle: (series?.year ?? local?.entry.year).map(String.init) ?? item.subtitle,
                           synopsis: series?.overview ?? item.synopsis, kind: .show,
                           artwork: artwork(series?.posterUrl ?? local?.entry.posterUrl) ?? item.artwork,
                           backdrop: nil, accent: item.accent, playable: false, playbackURL: nil,
                           requestState: state, tmdbID: tmdbId), options, local?.acquisitionProgress(entryID: localID))
    }

    private func libraryEntry(for item: MediaTitle) async throws -> TVEntry? {
        guard let id = entryID(for: item) else { return nil }
        return try await call("catalog.tvEntry", EntryID(entryId: id))
    }

    private func entryID(for item: MediaTitle) -> Int? {
        let prefix = "show:entry:"
        guard item.id.hasPrefix(prefix), let id = Int(item.id.dropFirst(prefix.count)), id > 0 else { return nil }
        return id
    }

    private func libraryTitles(_ library: Library) -> [MediaTitle] {
        library.movies.map { title($0.tmdbId, $0.title, $0.year, $0.overview, $0.posterUrl, kind: .movie, state: .available) }
            + library.tv.map { libraryTitle($0.key, $0.title, $0.year.flatMap(Int.init(exactly:)), $0.posterUrl, entryID: $0.entryId, state: .available) }
    }

    private func title(_ tmdbId: Int, _ title: String, _ year: Int?, _ overview: String?, _ poster: String?, kind: MediaKind, state: RequestState) -> MediaTitle {
        let movie = kind == .movie
        return MediaTitle(id: "\(movie ? "movie" : "show"):tmdb:\(tmdbId)", title: title,
                          subtitle: year.map(String.init) ?? "", synopsis: overview ?? "", kind: kind,
                          artwork: artwork(poster), backdrop: nil, accent: movie ? "#D69A5B" : "#65B4AC",
                          playable: false, playbackURL: nil, requestState: state, tmdbID: tmdbId)
    }

    private func libraryTitle(_ key: String, _ title: String, _ year: Int?, _ poster: String?, entryID: Int, state: RequestState) -> MediaTitle {
        MediaTitle(id: "show:entry:\(entryID)", title: title, subtitle: year.map(String.init) ?? "",
                   synopsis: "", kind: .show, artwork: artwork(poster), backdrop: nil, accent: "#65B4AC",
                   playable: false, playbackURL: nil, requestState: state, tmdbID: key.hasPrefix("tmdb:") ? Int(key.dropFirst(5)) : nil)
    }

    private func unique(_ items: [MediaTitle]) -> [MediaTitle] {
        var ids = Set<String>()
        var knownTV = Set<Int>()
        return items.filter { item in
            guard ids.insert(item.id).inserted else { return false }
            guard item.kind == .show, let tmdbID = item.tmdbID else { return true }
            return knownTV.insert(tmdbID).inserted
        }
    }

    private func artwork(_ raw: String?) -> URL? {
        guard let raw, let url = URL(string: raw), url.scheme?.lowercased() == "https", url.host != nil,
              url.user == nil, url.password == nil else { return nil }
        return url
    }

    func call<Params: Encodable, Value: Decodable>(_ method: String, _ params: Params, requiresSession: Bool = true) async throws -> Value {
        let id = UUID().uuidString
        var request = try makeRequest(path: "/rpc", requiresSession: requiresSession)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.httpBody = try JSONEncoder().encode(RPCRequest(id: id, method: method, params: params))

        let (data, response) = try await Self.session.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw ClientError.invalidResponse }
        if response.statusCode == 401 { throw ClientError.unauthorized }
        guard response.statusCode == 200 else { throw ClientError.http(response.statusCode) }
        let envelope: RPCResponse<Value>
        do { envelope = try JSONDecoder().decode(RPCResponse<Value>.self, from: data) }
        catch { throw ClientError.invalidResponse }
        guard envelope.jsonrpc == "2.0", envelope.id == id else { throw ClientError.invalidResponse }
        switch (envelope.result, envelope.error) {
        case (let value?, nil): return value
        case (nil, let error?):
            if error.code == -32001 { throw ClientError.unauthorized }
            throw ClientError.rpc(error.code)
        default: throw ClientError.invalidResponse
        }
    }

    func authenticatedRequest(path: String, query: [URLQueryItem] = []) throws -> URLRequest {
        try makeRequest(path: path, requiresSession: true, query: query)
    }

    private func makeRequest(path: String, requiresSession: Bool, query: [URLQueryItem] = []) throws -> URLRequest {
        guard var endpoint = URLComponents(url: baseURL, resolvingAgainstBaseURL: true),
              let scheme = endpoint.scheme?.lowercased(), let host = endpoint.host?.lowercased(), !host.isEmpty,
              endpoint.user == nil, endpoint.password == nil, endpoint.query == nil, endpoint.fragment == nil,
              scheme == "https" || (scheme == "http" && ["localhost", "127.0.0.1", "::1", "[::1]"].contains(host)) else {
            throw ClientError.invalidBaseURL
        }
        endpoint.path = path
        if !query.isEmpty { endpoint.queryItems = query }
        guard let url = endpoint.url else { throw ClientError.invalidBaseURL }
        var request = URLRequest(url: url)
        guard requiresSession else { return request }
        guard !token.isEmpty, token.utf8.allSatisfy({ (33...126).contains($0) }) else {
            throw ClientError.unauthorized
        }
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        return request
    }

    private static let session = makeSession(timeout: 30)

    static func makeSession(timeout: TimeInterval) -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.httpCookieStorage = nil
        configuration.urlCache = nil
        configuration.timeoutIntervalForRequest = timeout
        return URLSession(configuration: configuration, delegate: NoRedirects(), delegateQueue: nil)
    }
}

private final class NoRedirects: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest, completionHandler: @escaping @Sendable (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

nonisolated enum ClientError: LocalizedError {
    case invalidBaseURL, unauthorized, invalidQuery, missingIdentity, invalidSelection, catalogUnavailable
    case invalidResponse, http(Int), rpc(Int)

    var errorDescription: String? {
        switch self {
        case .invalidBaseURL: return "Use an HTTPS Podible address, or HTTP on loopback for development."
        case .unauthorized: return "Sign in to Podible again."
        case .invalidQuery: return "Enter a search of at most 200 characters."
        case .missingIdentity: return "This title has no requestable catalog identity."
        case .invalidSelection: return "Select at least one season or special, up to 500 of each."
        case .catalogUnavailable: return "Part of the catalog is unavailable. Try again later."
        case .invalidResponse: return "Podible returned an unexpected response."
        case .http(let code): return "Podible returned HTTP \(code)."
        case .rpc(-32003): return "This Podible account cannot access that item."
        case .rpc(-32602): return "Podible rejected the selection. Refresh the title and try again."
        case .rpc(let code): return "Podible could not complete the operation (\(code))."
        }
    }
}

nonisolated private struct RPCRequest<Params: Encodable>: Encodable {
    let jsonrpc = "2.0"
    let id: String
    let method: String
    let params: Params
}

nonisolated private struct RPCResponse<Value: Decodable>: Decodable {
    struct Failure: Decodable { let code: Int }
    let jsonrpc: String
    let id: String?
    let result: Value?
    let error: Failure?
}

nonisolated private struct Empty: Encodable {}
private extension MovieDetail.RequestsValue {
    var progress: TitleAcquisitionProgress {
        switch (state, stage) {
        case (_, "downloading"): .init(step: .download, status: "Downloading", transferScope: Int(exactly: id).map { .init(media: .movie, id: $0) })
        case _ where available: .init(step: .plex, status: "Ready in Plex")
        case ("waiting-for-release", _): .init(step: .search, status: "Watching for a release")
        case ("blocked", _): .init(step: .requested, status: "Acquisition stopped")
        case (_, "verifying"): .init(step: .verify, status: "Checking the download")
        case (_, "publishing"): .init(step: .verify, status: "Adding to Plex")
        case (_, "searching"), (_, "selecting"), (_, "replacing"): .init(step: .search, status: "Finding a release")
        default: .requested
        }
    }
}

private extension TVEntry {
    func acquisitionProgress(entryID: Int?) -> TitleAcquisitionProgress? {
        let numbers = Set(requests.flatMap(\.seasons).compactMap(Int.init(exactly:)))
        let seasons = progress.seasons.filter { numbers.contains($0.seasonNumber) }
        guard !seasons.isEmpty else { return nil }
        let available = seasons.reduce(0) { $0 + $1.available }
        let total = seasons.reduce(0) { $0 + $1.total }
        let stages = Set(seasons.map(\.stage))
        let stage = ["downloading", "verifying", "publishing", "pack-pending", "searching", "queued", "failed",
                     "waiting", "upcoming"].first(where: stages.contains)
        let count = "\(available) of \(total) episodes ready"
        let status: String
        switch stage {
        case "downloading": status = "Downloading episodes"
        case "verifying": status = "Checking episodes"
        case "publishing": status = "Adding episodes to Plex"
        case "pack-pending": status = "Waiting for a pack to finish"
        case "searching": status = "Finding episodes"
        case "failed": status = "Acquisition stopped for some episodes"
        case "waiting": status = "Watching for releases"
        case "upcoming": status = "Waiting for episodes to air"
        default: status = "Request received"
        }
        if total > 0 && available == total { return .init(step: .plex, status: "Ready in Plex", episodeCount: count) }
        let step: TitleAcquisitionProgress.Step = stage == "upcoming" ? .search : .from(stage: stage)
        return .init(step: step, status: status, episodeCount: count, transferScope: stage == "downloading"
            ? entryID.map { .init(media: .tv, id: $0) } : nil)
    }
}
