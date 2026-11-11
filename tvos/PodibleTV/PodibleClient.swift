import Foundation

struct PodibleClient {
    private let baseURL: URL
    private let token: String

    init(baseURL: URL, token: String) {
        self.baseURL = baseURL
        self.token = token
    }

    func browse() async throws -> [MediaTitle] {
        async let library: Library = call("catalog.available", LibraryQuery(query: ""))
        async let discovery: Discovery = call("catalog.discovery", Empty())
        let (published, trending) = try await (library, discovery)
        return unique(libraryTitles(published)
            + trending.movies.items.map { title($0, kind: .movie, state: .requestable) }
            + trending.tv.items.map { title($0, kind: .show, state: .requestable) })
    }

    func search(_ query: String) async throws -> [MediaTitle] {
        let query = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return [] }
        guard query.utf16.count <= 200 else { throw ClientError.invalidQuery }

        async let library: Library = call("catalog.available", LibraryQuery(query: query))
        async let movies = searchMovies(query)
        async let tv: TVSearch = call("tv.search", SearchQuery(query: query))
        let (published, films, shows) = try await (library, movies, tv)
        let results = unique(libraryTitles(published)
            + films.results.map { title($0, kind: .movie, state: films.canRequest ? .requestable : .unavailable) }
            + shows.results.map { title($0, kind: .show, state: .requestable) })
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
            let progress = active.first(where: \.available) ?? active.first
            return (title(movie.movie, kind: .movie, state: state), [], progress?.progress)
        case .show:
            return try await tvDetail(item)
        }
    }

    func request(_ item: MediaTitle, selection: Set<MediaRequestOption.ID>) async throws {
        switch item.kind {
        case .movie:
            guard let tmdbId = item.tmdbID, tmdbId > 0 else { throw ClientError.missingIdentity }
            let _: Requested<MovieRequest> = try await call("movies.request", MovieID(tmdbId: tmdbId))
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
            let _: Requested<TVRequestReceipt> = try await call(
                "tv.request", TVRequestSelection(tmdbId: tmdbId, seasonNumbers: seasons.isEmpty ? nil : seasons,
                                                 specialEpisodeKeys: specials.isEmpty ? nil : specials)
            )
        }
    }

    func requests() async throws -> [MediaTitle] {
        async let movies: RequestList<MovieRequest> = call("requests.list", Empty())
        async let tv: RequestList<TVRequestCard> = call("tv.requestCards", Empty())
        let (films, shows) = try await (movies, tv)
        return unique(films.requests.filter { $0.state != "withdrawn" && !$0.available }.map {
            title($0.movie, kind: .movie, state: .requested)
        } + shows.requests.filter { $0.intentionallyAbsent || $0.availableEpisodes < $0.requestedEpisodes }.map {
            libraryTitle($0.entry, entryID: $0.entryId, state: $0.intentionallyAbsent ? .unavailable : .requested)
        })
    }

    private func searchMovies(_ query: String) async throws -> (results: [CatalogTitle], canRequest: Bool, failed: Bool) {
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
            }
        }

        let coverage = remote?.seasons ?? []
        let specials = remote?.specials ?? []
        let seasons = remote?.series?.seasons.filter { $0.seasonNumber != 0 || specials.isEmpty }.map { season in
            let current = coverage.first { $0.number == season.seasonNumber }
            let ready = season.episodeCount > 0 && current?.available == season.episodeCount
            let requested = current?.requested == true
            let canRequest = season.seasonNumber > 0 && season.requestable && season.episodeCount > 0 && !ready && !requested
            let summary: String
            if ready { summary = "Ready in Plex" }
            else if requested { summary = "Already requested" }
            else if !season.requestable { summary = season.unavailableReason ?? "Season unavailable" }
            else { summary = "\(current?.available ?? 0) of \(season.episodeCount) episodes ready" }
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
                           requestState: state, tmdbID: tmdbId), options, local?.acquisitionProgress)
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
        library.movies.map { title($0, kind: .movie, state: .available) }
            + library.tv.map { libraryTitle($0.card, entryID: $0.entryId, state: .available) }
    }

    private func title(_ item: CatalogTitle, kind: MediaKind, state: RequestState) -> MediaTitle {
        let movie = kind == .movie
        return MediaTitle(id: "\(movie ? "movie" : "show"):tmdb:\(item.tmdbId)", title: item.title,
                          subtitle: item.year.map(String.init) ?? "", synopsis: item.overview ?? "", kind: kind,
                          artwork: artwork(item.posterUrl), backdrop: nil, accent: movie ? "#D69A5B" : "#65B4AC",
                          playable: false, playbackURL: nil, requestState: state, tmdbID: item.tmdbId)
    }

    private func libraryTitle(_ item: TVCard, entryID: Int, state: RequestState) -> MediaTitle {
        MediaTitle(id: "show:entry:\(entryID)", title: item.title, subtitle: item.year.map(String.init) ?? "",
                   synopsis: "", kind: .show, artwork: artwork(item.posterUrl), backdrop: nil, accent: "#65B4AC",
                   playable: false, playbackURL: nil, requestState: state, tmdbID: item.tmdbId)
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

    private func call<Params: Encodable, Value: Decodable>(_ method: String, _ params: Params) async throws -> Value {
        let id = UUID().uuidString
        var request = try authenticatedRequest(path: "/rpc")
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

    func authenticatedRequest(path: String) throws -> URLRequest {
        guard var endpoint = URLComponents(url: baseURL, resolvingAgainstBaseURL: true),
              let scheme = endpoint.scheme?.lowercased(), let host = endpoint.host?.lowercased(), !host.isEmpty,
              endpoint.user == nil, endpoint.password == nil, endpoint.query == nil, endpoint.fragment == nil,
              scheme == "https" || (scheme == "http" && ["localhost", "127.0.0.1", "::1", "[::1]"].contains(host)) else {
            throw ClientError.invalidBaseURL
        }
        endpoint.path = path
        guard let url = endpoint.url else { throw ClientError.invalidBaseURL }
        guard !token.isEmpty, token.utf8.allSatisfy({ (33...126).contains($0) }) else {
            throw ClientError.unauthorized
        }
        var request = URLRequest(url: url)
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
nonisolated private struct SearchQuery: Encodable { let query: String }
nonisolated private struct LibraryQuery: Encodable { let query: String; let limit = 50 }
nonisolated private struct MovieID: Encodable { let tmdbId: Int }
nonisolated private struct EntryID: Encodable { let entryId: Int }
nonisolated private struct TVRequestSelection: Encodable {
    let tmdbId: Int
    let seasonNumbers: [Int]?
    let specialEpisodeKeys: [String]?
}

nonisolated private struct CatalogTitle: Decodable, Sendable {
    let tmdbId: Int
    let title: String
    let year: Int?
    let overview: String?
    let posterUrl: String?
}

nonisolated private struct TVCard: Decodable, Sendable {
    let key: String
    let title: String
    let year: Int?
    let posterUrl: String?
    let entryId: Int?

    var tmdbId: Int? { key.hasPrefix("tmdb:") ? Int(key.dropFirst(5)) : nil }
}

nonisolated private struct Library: Decodable, Sendable {
    struct Show: Decodable, Sendable {
        let entryId: Int
        let card: TVCard

        init(from decoder: any Decoder) throws {
            card = try TVCard(from: decoder)
            guard let id = card.entryId else {
                throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "Missing TV entry ID"))
            }
            entryId = id
        }
    }
    let movies: [CatalogTitle]
    let tv: [Show]
}

nonisolated private struct Discovery: Decodable, Sendable {
    struct Section: Decodable, Sendable { let items: [CatalogTitle] }
    let movies: Section
    let tv: Section
}

nonisolated private struct MovieStatus: Decodable, Sendable { let catalogConfigured: Bool; let acquisitionConfigured: Bool }
nonisolated private struct MovieSearch: Decodable, Sendable { let results: [CatalogTitle] }
nonisolated private struct MovieDetail: Decodable, Sendable {
    let movie: CatalogTitle
    let requests: [MovieRequest]
    let availableInLibrary: Bool
}
nonisolated private struct MovieRequest: Decodable, Sendable {
    let movie: CatalogTitle
    let state: String
    let available: Bool
    let stage: String?
    let download: TitleAcquisitionProgress.Download?

    var progress: TitleAcquisitionProgress {
        switch (state, stage) {
        case _ where available: .init(step: .plex, status: "Ready in Plex")
        case ("waiting-for-release", _): .init(step: .search, status: "Watching for a release")
        case ("blocked", _): .init(step: .requested, status: "Acquisition stopped")
        case (_, "downloading"): .init(step: .download, status: "Downloading", download: download)
        case (_, "verifying"): .init(step: .verify, status: "Checking the download")
        case (_, "publishing"): .init(step: .verify, status: "Adding to Plex")
        case (_, "searching"), (_, "selecting"), (_, "replacing"):
            .init(step: .search, status: "Finding a release")
        default: .requested
        }
    }
}
nonisolated private struct RequestList<Entry: Decodable & Sendable>: Decodable, Sendable { let requests: [Entry] }
nonisolated private struct Requested<Entry: Decodable>: Decodable { let request: Entry }
nonisolated private struct TVRequestReceipt: Decodable { let requestId: Int }
nonisolated private struct TVRequestCard: Decodable, Sendable {
    let entryId: Int
    let entry: TVCard
    let requestedEpisodes: Int
    let availableEpisodes: Int
    let intentionallyAbsent: Bool
}
nonisolated private struct TVSearch: Decodable, Sendable {
    let configured: Bool
    let unavailableReason: String?
    let results: [CatalogTitle]
}
nonisolated private struct TVDetail: Decodable {
    struct Series: Decodable {
        struct Season: Decodable {
            let seasonNumber: Int
            let title: String
            let episodeCount: Int
            let requestable: Bool
            let unavailableReason: String?
        }
        let title: String
        let year: Int?
        let overview: String
        let posterUrl: String?
        let seasons: [Season]
    }
    struct Coverage: Decodable { let number: Int; let available: Int; let requested: Bool }
    struct Special: Decodable {
        let episodeKey: String
        let episodeNumber: Int
        let title: String
        let airDate: String?
        let available: Bool
        let requested: Bool
    }
    let showId: Int?
    let series: Series?
    let seasons: [Coverage]
    let specials: [Special]?
}
nonisolated private struct TVEntry: Decodable {
    struct Metadata: Decodable { let tmdbId: Int?; let title: String; let year: Int?; let posterUrl: String? }
    struct Progress: Decodable {
        struct Season: Decodable { let seasonNumber: Int; let available: Int; let total: Int; let stage: String }
        let download: TitleAcquisitionProgress.Download?
        let seasons: [Season]
    }
    struct OwnRequest: Decodable { let id: Int; let seasons: [Int] }
    let entry: Metadata
    let availableEpisodes: Int
    let progress: Progress
    let requests: [OwnRequest]

    var acquisitionProgress: TitleAcquisitionProgress? {
        let numbers = Set(requests.flatMap(\.seasons))
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
        return .init(step: step, status: status, episodeCount: count, download: stage == "downloading" ? progress.download : nil)
    }
}
