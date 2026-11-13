import AVKit
import SwiftUI

@main
struct PodibleTVApp: App {
    var body: some Scene {
        WindowGroup { TVRoot().preferredColorScheme(.dark) }
    }
}

private struct TVRoot: View {
    @State private var connection = Connection.loadForTV()
    @State private var catalog: [MediaTitle] = []
    @State private var serverRequests: [MediaTitle] = []
    @State private var requestsError: String?
    @State private var loadingRequests = true
    @State private var sent: [MediaTitle] = []
    @State private var demoSelections: [String: Set<MediaRequestOption.ID>] = [:]
    @State private var selection: Section = .browse
    @State private var searchQuery = ""
    @State private var path: [MediaTitle] = []
    @State private var playback: Playback?
    @State private var browseError: String?
    @State private var loading = true
    @FocusState private var sidebarFocus: Section?
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    private enum Section: String, CaseIterable {
        case browse = "Home", search = "Search", movies = "Movies", tv = "TV Shows"
        case requests = "Requests", settings = "Settings"

        var icon: String {
            switch self {
            case .browse: "house"
            case .search: "magnifyingglass"
            case .movies: "film"
            case .tv: "tv"
            case .requests: "tray"
            case .settings: "gearshape"
            }
        }
    }

    var body: some View {
        NavigationStack(path: $path) {
            ZStack(alignment: .leading) {
                Group {
                    switch selection {
                    case .browse, .movies, .tv: browse.id(selection)
                    case .search: SearchScreen(catalog: catalog, connection: connection, open: open, query: $searchQuery)
                    case .requests:
                        requests.task(id: connection) { await refreshRequests() }
                    case .settings:
                        ConnectionScreen(connection: connection) {
                            connection = $0
                            catalog = []
                            serverRequests = []
                            loading = true
                            sent = []
                            selection = .browse
                        }
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .focusSection()
                .padding(.leading, 84)
                Color.black.opacity(sidebarFocus == nil ? 0 : 0.55)
                    .ignoresSafeArea()
                    .allowsHitTesting(false)
                    .accessibilityHidden(true)
                sidebar
            }
            .animation(reduceMotion ? nil : .easeOut(duration: 0.18), value: sidebarFocus != nil)
            .foregroundStyle(TVPalette.paper)
            .background(TVPalette.background.ignoresSafeArea())
            .navigationDestination(for: MediaTitle.self) { item in
                DetailScreen(initial: item, connection: connection, demoSelection: demoSelections[item.id] ?? [],
                             play: { playback = Playback(url: $0) },
                             requested: { requested, selection in
                                 if let index = catalog.firstIndex(where: { $0.id == requested.id }),
                                    catalog[index].requestState != .available {
                                     catalog[index] = requested
                                 }
                                 if connection == nil {
                                     sent.removeAll { $0.id == requested.id }
                                     sent.append(requested)
                                     demoSelections[item.id, default: []].formUnion(selection)
                                 }
                             })
            }
        }
        .task(id: connection) { await refresh() }
        .fullScreenCover(item: $playback) { playback in DemoPlayer(url: playback.url) }
    }

    private var sidebar: some View {
        let expanded = sidebarFocus != nil
        return VStack(spacing: 10) {
            ForEach(Section.allCases, id: \.self) { section in
                let focused = sidebarFocus == section
                Button { selection = section } label: {
                    Image(systemName: section.icon)
                        .font(.system(size: 30, weight: selection == section ? .semibold : .regular))
                        .frame(width: 64, height: 62)
                        .overlay(alignment: .leading) {
                            if expanded {
                                Text(section.rawValue)
                                    .font(.system(size: 25, weight: focused ? .bold : .medium))
                                    .fixedSize()
                                    .offset(x: 74)
                                    .accessibilityHidden(true)
                            }
                        }
                        .foregroundStyle(focused ? TVPalette.background :
                            (selection == section ? TVPalette.accent : TVPalette.paper.opacity(0.72)))
                        .background(alignment: .leading) {
                            RoundedRectangle(cornerRadius: 12)
                                .fill(focused ? TVPalette.paper : .clear)
                                .frame(width: expanded ? 254 : 64)
                        }
                }
                .buttonStyle(TVUnstyledButton())
                .focusEffectDisabled()
                .focused($sidebarFocus, equals: section)
                .accessibilityLabel(section.rawValue)
                .accessibilityIdentifier("nav.\(section.rawValue)")
                .accessibilityAddTraits(selection == section ? .isSelected : [])
            }
        }
        .frame(width: 84)
        .frame(maxHeight: .infinity)
        .background(alignment: .leading) {
            Rectangle().fill(TVPalette.background)
                .frame(width: expanded ? 366 : 164)
                .offset(x: -80)
                .ignoresSafeArea(edges: .vertical)
        }
        .overlay(alignment: .topLeading) {
            Text(expanded ? "podible" : "p")
                .font(.system(size: 42, weight: .heavy, design: .serif))
                .foregroundStyle(TVPalette.accent)
                .fixedSize()
                .padding(.leading, 20)
                .padding(.top, 12)
                .accessibilityHidden(true)
        }
        .focusSection()
    }

    @ViewBuilder private var browse: some View {
        let items = catalog.filter {
            selection == .browse || $0.kind == (selection == .movies ? .movie : .show)
        }
        if loading {
            ProgressView("Loading library")
        } else if let browseError {
            VStack(spacing: 22) {
                Text(browseError)
                Button("Try Again") { Task { await refresh() } }
                Button("Connection Settings") { selection = .settings }
            }
        } else if items.isEmpty {
            ContentUnavailableView("Nothing to browse yet", systemImage: "play.rectangle",
                                   description: Text("Try searching for a movie or show."))
        } else {
            BrowseView(items: items, open: open)
        }
    }

    private func open(_ item: MediaTitle) { path.append(item) }

    @ViewBuilder private var requests: some View {
        if connection != nil && loadingRequests && serverRequests.isEmpty {
            ProgressView("Loading requests")
        } else if connection != nil, let requestsError {
            VStack(spacing: 22) {
                Text(requestsError)
                Button("Try Again") { Task { await refreshRequests() } }
            }
        } else {
            let entries = connection == nil ? sent : serverRequests
            if entries.isEmpty {
                ContentUnavailableView("No requests to show", systemImage: "tray",
                                       description: Text("Find a movie or a TV season in Search."))
            } else {
                titleList(entries)
            }
        }
    }

    @MainActor private func refreshRequests() async {
        guard let connection else { return }
        loadingRequests = true
        requestsError = nil
        defer { if self.connection == connection { loadingRequests = false } }
        do {
            let result = try await PodibleClient(baseURL: connection.url, token: connection.token).requests()
            guard !Task.isCancelled, self.connection == connection else { return }
            serverRequests = result
        } catch {
            guard !Task.isCancelled, self.connection == connection else { return }
            requestsError = error.localizedDescription
        }
    }

    @MainActor private func refresh() async {
        guard let connection else {
            catalog = DemoCatalog.items
            demoSelections = [:]
            browseError = nil
            loading = false
            return
        }
        loading = true
        browseError = nil
        defer { if self.connection == connection { loading = false } }
        do {
            let result = try await PodibleClient(baseURL: connection.url, token: connection.token).browse()
            guard !Task.isCancelled, self.connection == connection else { return }
            catalog = result
        } catch {
            guard !Task.isCancelled, self.connection == connection else { return }
            catalog = []
            browseError = error.localizedDescription
        }
    }

    private func titleList(_ items: [MediaTitle]) -> some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 16) {
                ForEach(items) { item in
                    Button { open(item) } label: {
                        HStack {
                            Text(item.title)
                            Spacer()
                            Text(item.requestState.label).foregroundStyle(.secondary)
                        }
                        .padding(.vertical, 10)
                    }
                }
            }.padding(.horizontal, 70)
        }
    }
}

private struct DetailScreen: View {
    @Environment(\.scenePhase) private var scenePhase
    @State private var observationRetry = 0
    @State private var item: MediaTitle
    @State private var seasons: [MediaRequestOption]
    @State private var progress: TitleAcquisitionProgress?
    @State private var loaded: Bool
    @State private var detailError: String?
    let connection: Connection?
    let play: (URL) -> Void
    let requested: (MediaTitle, Set<MediaRequestOption.ID>) -> Void

    init(initial: MediaTitle, connection: Connection?, demoSelection: Set<MediaRequestOption.ID>,
         play: @escaping (URL) -> Void, requested: @escaping (MediaTitle, Set<MediaRequestOption.ID>) -> Void) {
        _item = State(initialValue: initial)
        _seasons = State(initialValue: connection == nil ? DemoCatalog.requestOptions(for: initial, requested: demoSelection) : [])
        _loaded = State(initialValue: connection == nil)
        _progress = State(initialValue: connection == nil && (!demoSelection.isEmpty || initial.requestState == .requested)
            ? .requested : nil)
        self.connection = connection
        self.play = play
        self.requested = requested
    }

    var body: some View {
        VStack {
            if let detailError {
                HStack {
                    Text(detailError)
                    Button("Try Again") { observationRetry += 1 }
                }.padding(.top, 20)
            }
            if !loaded && detailError == nil { ProgressView("Loading title") }
            if loaded {
                TitleDetailView(item: item, seasons: seasons, progress: progress, play: play, request: submit)
            }
        }
        .environment(\.trailerConnection, connection)
        .task(id: ObservationID(connection: connection, retry: observationRetry, background: scenePhase == .background)) {
            guard scenePhase != .background, let connection else { return }
            do {
                if !loaded || detailError != nil { try await load() }
                let client = PodibleClient(baseURL: connection.url, token: connection.token)
                try await AcquisitionChanges(client: client).observe { try await load() }
            } catch {
                guard !Task.isCancelled else { return }
                detailError = error.localizedDescription
            }
        }
    }

    private struct ObservationID: Equatable {
        let connection: Connection?
        let retry: Int
        let background: Bool
    }

    @MainActor private func load() async throws {
        guard let connection else { return }
        detailError = nil
        do {
            let details = try await PodibleClient(baseURL: connection.url, token: connection.token).detail(item)
            guard !Task.isCancelled else { return }
            (item, seasons, progress) = details
            loaded = true
        } catch ClientError.unauthorized {
            throw ClientError.unauthorized
        } catch {
            guard !Task.isCancelled else { return }
            detailError = error.localizedDescription
        }
    }

    @MainActor private func submit(_ selection: Set<MediaRequestOption.ID>) async throws {
        if let connection {
            try await PodibleClient(baseURL: connection.url, token: connection.token).request(item, selection: selection)
        }
        for i in seasons.indices where selection.contains(seasons[i].id) && seasons[i].requestable {
            seasons[i] = MediaRequestOption(id: seasons[i].id, name: seasons[i].name, summary: "Requested", requestable: false)
        }
        if item.requestState != .available { item.requestState = .requested }
        if progress == nil || (connection == nil && progress?.step == .plex) { progress = .requested }
        var requestItem = item
        requestItem.requestState = .requested
        requested(requestItem, selection)
    }
}

private struct SearchScreen: View {
    let catalog: [MediaTitle]
    let connection: Connection?
    let open: (MediaTitle) -> Void
    @Binding var query: String
    @State private var results: [MediaTitle] = []
    @State private var status: String?
    @FocusState private var focusedPoster: String?

    private var term: String { query.trimmingCharacters(in: .whitespacesAndNewlines) }

    var body: some View {
        ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    if let status { Text(status).foregroundStyle(.secondary) }
                    let titles = term.isEmpty ? Array(catalog.prefix(8)) : results
                    if !titles.isEmpty {
                        Text(term.isEmpty ? "Browse Titles" : "Results")
                            .font(.system(size: 30, weight: .semibold))
                            .accessibilityAddTraits(.isHeader)
                        LazyVGrid(columns: [GridItem(.adaptive(minimum: 180, maximum: 200), spacing: 24)],
                                  alignment: .leading, spacing: 25) {
                            ForEach(titles) { item in
                                TVPosterButton(item: item, width: 176, focused: focusedPoster == item.id) { open(item) }
                                    .focused($focusedPoster, equals: item.id)
                            }
                        }
                        .padding(.horizontal, 5)
                    }
                }
                .padding(.vertical, 12)
        }
        .scrollIndicators(.hidden)
        .focusSection()
        .padding(.horizontal, 72)
        .padding(.top, 26)
        .searchable(text: $query, prompt: "Search movies and TV")
        .task(id: term) { await search(term) }
    }

    @MainActor private func search(_ term: String) async {
        status = nil
        guard !term.isEmpty else { results = []; return }
        do {
            let found: [MediaTitle]
            if let connection {
                try await Task.sleep(for: .milliseconds(350))
                status = "Searching..."
                found = try await PodibleClient(baseURL: connection.url, token: connection.token).search(term)
            } else {
                found = catalog.filter { $0.title.localizedCaseInsensitiveContains(term) }
            }
            try Task.checkCancellation()
            results = found
            status = found.isEmpty ? "No matches for this search." : nil
        } catch {
            guard !Task.isCancelled else { return }
            status = error.localizedDescription
        }
    }
}

private struct ConnectionScreen: View {
    let connection: Connection?
    let changed: (Connection?) -> Void
    @State private var server = Connection.rememberedServer
    @State private var status: String?
    @State private var pairingBase: URL?
    @State private var pairing: DeviceLoginGrant?

    var body: some View {
        VStack(alignment: .leading, spacing: 22) {
            Text("Connect to Podible").font(.title2.bold())
            if let connection {
                Text("Signed in to \(connection.url.host ?? "Podible")")
                Button("Disconnect") { Task { await disconnect(connection) } }
            } else if let pairingBase {
                if let pairing, let url = URL(string: pairing.authorizePath, relativeTo: pairingBase)?.absoluteURL {
                    HStack(alignment: .center, spacing: 48) {
                        PairingQRCode(url: url)
                        VStack(alignment: .leading, spacing: 14) {
                            Text("Scan with your phone").font(.headline)
                            Text("Approve the sign-in on your phone.")
                                .font(.caption).foregroundStyle(.secondary)
                        }
                    }
                } else {
                    ProgressView("Starting sign-in…")
                }
                Button("Cancel") { self.pairingBase = nil; pairing = nil }
            } else {
                Text("Enter your server address, then sign in on your phone.")
                    .font(.caption).foregroundStyle(.secondary)
                TextField("https://podible.example", text: $server)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .keyboardType(.URL)
                    .accessibilityIdentifier("device-sign-in.server")
                Button("Sign in with phone") {
                    status = nil
                    do {
                        let base = try Connection.serverURL(server)
                        Connection.rememberServer(base)
                        pairingBase = base
                    }
                    catch { status = error.localizedDescription }
                }
            }
            if let status { Text(status).font(.caption) }
        }
        .frame(maxWidth: 1140, alignment: .leading)
        .padding(70)
        .task(id: pairingBase) { if let pairingBase { await connect(pairingBase) } }
    }

    @MainActor private func connect(_ base: URL) async {
        let client = PodibleClient(baseURL: base, token: "")
        do {
            let grant = try await client.beginPairing()
            try Task.checkCancellation()
            pairing = grant
            let clock = ContinuousClock()
            let deadline = clock.now.advanced(by: .seconds(grant.expiresInSeconds))
            while clock.now < deadline {
                try await Task.sleep(for: .seconds(3))
                let result: DeviceLoginExchange
                do { result = try await client.exchangePairing(grant) }
                catch is URLError { continue }
                try Task.checkCancellation()
                switch result.status {
                case .pending: continue
                case .unavailable: break
                case .approved:
                    guard let token = result.accessToken else { throw ClientError.invalidResponse }
                    let value = try Connection(server: base.absoluteString, token: token, paired: true)
                    try value.save()
                    pairing = nil
                    pairingBase = nil
                    changed(value)
                    return
                }
                break
            }
            status = "This sign-in expired. Start a new one."
        } catch {
            guard !Task.isCancelled else { return }
            status = error.localizedDescription
        }
        pairing = nil
        pairingBase = nil
    }

    @MainActor private func disconnect(_ connection: Connection) async {
        do {
            if connection.paired {
                do { try await PodibleClient(baseURL: connection.url, token: connection.token).signOut() }
                catch ClientError.unauthorized { /* Session already expired or revoked. */ }
            }
            Connection.rememberServer(connection.url)
            try Connection.remove()
            changed(nil)
        } catch { status = error.localizedDescription }
    }
}

private struct Playback: Identifiable {
    var id: URL { url }
    let url: URL
}

private struct DemoPlayer: View {
    @State private var player: AVPlayer

    init(url: URL) { _player = State(initialValue: AVPlayer(url: url)) }

    var body: some View {
        VideoPlayer(player: player)
            .ignoresSafeArea()
            .onAppear { player.play() }
            .onDisappear { player.pause() }
    }
}
