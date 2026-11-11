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
                    case .search: SearchScreen(catalog: catalog, connection: connection, open: open)
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
    @State private var query = ""
    @State private var results: [MediaTitle] = []
    @State private var status: String?
    @State private var submittedQuery: String?
    @State private var searchTask: Task<Void, Never>?
    @FocusState private var focusedPoster: String?
    @FocusState private var queryFocused: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 22) {
            VStack(alignment: .leading, spacing: 18) {
                Text("Search movies and TV")
                    .font(.system(size: 46, weight: .semibold, design: .serif))
                    .accessibilityAddTraits(.isHeader)
                HStack(spacing: 24) {
                    TextField("Title", text: $query, prompt: Text("Enter a title")
                        .foregroundStyle(queryFocused ? TVPalette.background.opacity(0.72) : TVPalette.paper.opacity(0.62)))
                        .foregroundStyle(queryFocused ? TVPalette.background : TVPalette.paper)
                        .focused($queryFocused)
                        .frame(width: 410)
                        .onSubmit(search)
                        .accessibilityIdentifier("search.query")
                    Button("Search", systemImage: "magnifyingglass", action: search)
                        .disabled(query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
            }
            .focusSection()

            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    if let status { Text(status).foregroundStyle(.secondary) }
                    let titles = submittedQuery == nil ? Array(catalog.prefix(8)) : results
                    if !titles.isEmpty {
                        Text(submittedQuery == nil ? "Browse Titles" : "Results")
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
        }
        .padding(.horizontal, 72)
        .padding(.top, 26)
        .onAppear { queryFocused = true }
        .onDisappear { searchTask?.cancel() }
    }

    private func search() {
        let term = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !term.isEmpty else { return }
        searchTask?.cancel()
        searchTask = Task { @MainActor in
            submittedQuery = term
            results = []
            status = "Searching..."
            do {
                let found: [MediaTitle]
                if let connection {
                    found = try await PodibleClient(baseURL: connection.url, token: connection.token).search(term)
                } else {
                    found = catalog.filter { $0.title.localizedCaseInsensitiveContains(term) }
                }
                guard !Task.isCancelled else { return }
                results = found
                status = found.isEmpty ? "No matches for this search." : nil
            } catch {
                guard !Task.isCancelled else { return }
                results = []
                status = error.localizedDescription
            }
        }
    }
}

private struct ConnectionScreen: View {
    let connection: Connection?
    let changed: (Connection?) -> Void
    @State private var server = ""
    @State private var token = ""
    @State private var status: String?
    @State private var connecting = false

    var body: some View {
        VStack(alignment: .leading, spacing: 22) {
            Text("Connect to Podible").font(.title2.bold())
            Text("Use a Podible HTTPS address and session token. Plex playback isn't connected in this build.")
                .font(.caption).foregroundStyle(.secondary)
            TextField("https://podible.example", text: $server)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
            SecureField("Session token", text: $token)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
            if let status { Text(status).font(.caption) }
            HStack(spacing: 24) {
                Button(connecting ? "Connecting..." : "Connect") { Task { await connect() } }
                    .disabled(connecting || server.isEmpty || token.isEmpty)
                if connection != nil {
                    Button("Disconnect") {
                        do { try Connection.remove(); changed(nil) }
                        catch { status = error.localizedDescription }
                    }
                }
            }
        }
        .frame(maxWidth: 950, alignment: .leading)
        .padding(70)
        .onAppear { server = connection?.url.absoluteString ?? "" }
    }

    @MainActor private func connect() async {
        connecting = true
        status = nil
        defer { connecting = false }
        do {
            let value = try Connection(server: server, token: token)
            _ = try await PodibleClient(baseURL: value.url, token: value.token).browse()
            try value.save()
            token = ""
            changed(value)
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
