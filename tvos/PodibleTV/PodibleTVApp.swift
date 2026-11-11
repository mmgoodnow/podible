import AVKit
import SwiftUI

@main
struct PodibleTVApp: App {
    var body: some Scene {
        WindowGroup { TVRoot().preferredColorScheme(.dark) }
    }
}

private struct TVRoot: View {
    @State private var connection = Connection.load()
    @State private var catalog: [MediaTitle] = []
    @State private var serverRequests: [MediaTitle] = []
    @State private var requestsError: String?
    @State private var loadingRequests = true
    @State private var sent: [MediaTitle] = []
    @State private var demoSeasons: [String: Set<Int>] = [:]
    @State private var selection: Section = .browse
    @State private var path: [MediaTitle] = []
    @State private var playback: Playback?
    @State private var browseError: String?
    @State private var loading = true

    private enum Section: String, CaseIterable {
        case browse = "Browse", search = "Search", requests = "Requests", settings = "Settings"
    }

    var body: some View {
        NavigationStack(path: $path) {
            VStack(spacing: 12) {
                HStack(spacing: 30) {
                    Text("podible").font(.system(size: 40, weight: .heavy, design: .serif))
                    if connection == nil {
                        Text("Demo").font(.caption2).foregroundStyle(.secondary)
                    }
                    Spacer()
                    ForEach(Section.allCases, id: \.self) { section in
                        Button(section.rawValue) { selection = section }
                            .buttonStyle(TVNavigationButton(selected: selection == section))
                            .accessibilityAddTraits(selection == section ? .isSelected : [])
                    }
                }
                .padding(.horizontal, 70)
                .padding(.vertical, 12)
                Group {
                    switch selection {
                    case .browse: browse
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
            }
            .background(Color(red: 0.024, green: 0.063, blue: 0.067).ignoresSafeArea())
            .navigationDestination(for: MediaTitle.self) { item in
                DetailScreen(initial: item, connection: connection, demoSeasons: demoSeasons[item.id] ?? [],
                             play: { playback = Playback(url: $0) },
                             requested: { requested, seasons in
                                 if let index = catalog.firstIndex(where: { $0.id == requested.id }),
                                    catalog[index].requestState != .available {
                                     catalog[index] = requested
                                 }
                                 if connection == nil {
                                     sent.removeAll { $0.id == requested.id }
                                     sent.append(requested)
                                     demoSeasons[item.id, default: []].formUnion(seasons)
                                 }
                             })
            }
        }
        .task(id: connection) { await refresh() }
        .fullScreenCover(item: $playback) { playback in DemoPlayer(url: playback.url) }
    }

    @ViewBuilder private var browse: some View {
        if loading {
            ProgressView("Loading library")
        } else if let browseError {
            VStack(spacing: 22) {
                Text(browseError)
                Button("Try Again") { Task { await refresh() } }
                Button("Connection Settings") { selection = .settings }
            }
        } else if catalog.isEmpty {
            ContentUnavailableView("Nothing to browse yet", systemImage: "play.rectangle",
                                   description: Text("Try searching for a movie or show."))
        } else {
            BrowseView(items: catalog, open: open)
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
            demoSeasons = [:]
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

private struct TVNavigationButton: ButtonStyle {
    let selected: Bool

    func makeBody(configuration: Configuration) -> some View {
        Label(configuration: configuration, selected: selected)
    }

    private struct Label: View {
        @Environment(\.isFocused) private var focused
        let configuration: ButtonStyle.Configuration
        let selected: Bool

        var body: some View {
            configuration.label
                .font(.system(size: 25, weight: .semibold))
                .foregroundStyle(focused ? Color.black : Color.white.opacity(selected ? 1 : 0.7))
                .padding(.horizontal, 23)
                .padding(.vertical, 12)
                .background(focused ? Color(red: 0.96, green: 0.85, blue: 0.62) :
                    selected ? Color.white.opacity(0.12) : .clear, in: Capsule())
                .scaleEffect(configuration.isPressed ? 0.96 : 1)
        }
    }
}

private struct DetailScreen: View {
    @State private var item: MediaTitle
    @State private var seasons: [MediaSeason]
    @State private var loaded: Bool
    @State private var detailError: String?
    let connection: Connection?
    let play: (URL) -> Void
    let requested: (MediaTitle, [Int]) -> Void

    init(initial: MediaTitle, connection: Connection?, demoSeasons: Set<Int>,
         play: @escaping (URL) -> Void, requested: @escaping (MediaTitle, [Int]) -> Void) {
        _item = State(initialValue: initial)
        _seasons = State(initialValue: connection == nil ? DemoCatalog.seasons(for: initial, requested: demoSeasons) : [])
        _loaded = State(initialValue: connection == nil)
        self.connection = connection
        self.play = play
        self.requested = requested
    }

    var body: some View {
        VStack {
            if let detailError {
                HStack {
                    Text(detailError)
                    Button("Try Again") { Task { await load() } }
                }.padding(.top, 20)
            }
            if !loaded && detailError == nil { ProgressView("Loading title") }
            if loaded {
                TitleDetailView(item: item, seasons: seasons, play: play, request: submit)
            }
        }
        .task { if !loaded { await load() } }
    }

    @MainActor private func load() async {
        guard let connection else { return }
        detailError = nil
        do {
            (item, seasons) = try await PodibleClient(baseURL: connection.url, token: connection.token).detail(item)
            loaded = true
        } catch {
            guard !Task.isCancelled else { return }
            detailError = error.localizedDescription
        }
    }

    @MainActor private func submit(_ numbers: [Int]) async throws {
        if let connection {
            try await PodibleClient(baseURL: connection.url, token: connection.token).request(item, seasons: numbers)
        }
        for i in seasons.indices where numbers.contains(seasons[i].id) {
            seasons[i] = MediaSeason(id: seasons[i].id, name: seasons[i].name, summary: "Requested", requestable: false)
        }
        if item.requestState != .available { item.requestState = .requested }
        var requestItem = item
        requestItem.requestState = .requested
        requested(requestItem, numbers)
    }
}

private struct SearchScreen: View {
    let catalog: [MediaTitle]
    let connection: Connection?
    let open: (MediaTitle) -> Void
    @State private var query = ""
    @State private var results: [MediaTitle] = []
    @State private var status: String?
    @State private var searchTask: Task<Void, Never>?

    var body: some View {
        VStack(alignment: .leading, spacing: 24) {
            HStack(spacing: 24) {
                TextField("Movie or TV show", text: $query).onSubmit(search)
                Button("Search", systemImage: "magnifyingglass", action: search)
                    .disabled(query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
            if let status { Text(status).foregroundStyle(.secondary) }
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 14) {
                    ForEach(results) { item in
                        Button { open(item) } label: {
                            HStack {
                                VStack(alignment: .leading) {
                                    Text(item.title)
                                    Text(item.subtitle).font(.caption2).foregroundStyle(.secondary)
                                }
                                Spacer()
                                Text(item.requestState.label).font(.caption)
                            }
                            .padding(.vertical, 8)
                        }
                    }
                }
            }
        }
        .padding(70)
        .onDisappear { searchTask?.cancel() }
    }

    private func search() {
        let term = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !term.isEmpty else { return }
        searchTask?.cancel()
        searchTask = Task { @MainActor in
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
