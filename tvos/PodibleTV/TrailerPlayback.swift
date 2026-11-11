import AVKit
import SwiftUI

extension EnvironmentValues {
    @Entry var trailerConnection: Connection? = nil
}

private struct TrailerRequest: Equatable {
    let connection: Connection
    let kind: MediaKind
    let tmdbID: Int

    private static let maxBytes = 32 * 1024 * 1024
    private static let session: URLSession = {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpShouldSetCookies = false
        configuration.timeoutIntervalForRequest = 12
        configuration.timeoutIntervalForResource = 40
        return URLSession(configuration: configuration, delegate: NoTrailerRedirects(), delegateQueue: nil)
    }()

    private func request(file: Bool) -> URLRequest {
        var url = connection.url.appending(path: "api/trailers/\(kind.rawValue)/\(tmdbID)")
        if file { url.append(path: "file") }
        var request = URLRequest(url: url)
        request.setValue("Bearer \(connection.token)", forHTTPHeaderField: "Authorization")
        return request
    }

    func isAvailable() async -> Bool {
        struct Availability: Decodable { let available: Bool }
        do {
            let (data, response) = try await Self.session.data(for: request(file: false))
            return (response as? HTTPURLResponse)?.statusCode == 200 &&
                (try? JSONDecoder().decode(Availability.self, from: data))?.available == true
        } catch { return false }
    }

    func download() async throws -> TrailerFile {
        let (temporary, response) = try await Self.session.download(for: request(file: true))
        defer { try? FileManager.default.removeItem(at: temporary) }
        try Task.checkCancellation()
        let size = try temporary.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
        guard let http = response as? HTTPURLResponse, http.statusCode == 200, http.mimeType == "video/mp4",
              (8...Self.maxBytes).contains(size), http.value(forHTTPHeaderField: "X-Podible-Trailer-Bytes") == String(size) else {
            throw URLError(.cannotDecodeContentData)
        }
        let handle = try FileHandle(forReadingFrom: temporary)
        defer { try? handle.close() }
        guard let prefix = try handle.read(upToCount: 8), prefix.count == 8,
              Data(prefix.suffix(4)) == Data("ftyp".utf8) else { throw URLError(.cannotDecodeContentData) }
        let file = TrailerFile()
        try FileManager.default.moveItem(at: temporary, to: file.url)
        return file
    }
}

private final class NoTrailerRedirects: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest) async -> URLRequest? {
        nil
    }
}

private final class TrailerFile: Identifiable {
    let url = FileManager.default.temporaryDirectory.appending(path: "trailer-\(UUID().uuidString).mp4")
    deinit { try? FileManager.default.removeItem(at: url) }
}

struct TrailerButton: View {
    private let request: TrailerRequest
    private let preferFocus: Bool
    @FocusState private var focused: Bool
    @State private var availableFor: TrailerRequest?
    @State private var preparing = false
    @State private var failed = false
    @State private var file: TrailerFile?

    init(connection: Connection, kind: MediaKind, tmdbID: Int, preferFocus: Bool) {
        request = TrailerRequest(connection: connection, kind: kind, tmdbID: tmdbID)
        self.preferFocus = preferFocus
    }

    var body: some View {
        ZStack {
            if availableFor == request {
                Button { preparing = true } label: {
                    Label(preparing ? "Preparing Trailer..." : "Watch Trailer", systemImage: "film")
                }
                .buttonStyle(.bordered)
                .focused($focused)
                .accessibilityIdentifier("trailer.watch")
                .task {
                    if preferFocus { focused = true }
                }
            }
        }
        .task(id: request) {
            let available = await request.isAvailable()
            if !Task.isCancelled { availableFor = available ? request : nil }
        }
        .task(id: preparing) {
            guard preparing else { return }
            defer { preparing = false }
            do {
                let downloaded = try await request.download()
                if !Task.isCancelled { file = downloaded }
            } catch {
                if !Task.isCancelled { failed = true }
            }
        }
        .fullScreenCover(item: $file, onDismiss: { focused = true }) { file in TrailerPlayback(file: file) }
        .alert("Trailer unavailable", isPresented: $failed) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Podible could not load this trailer. Try again later.")
        }
    }
}

private struct TrailerPlayback: View {
    let file: TrailerFile
    @Environment(\.dismiss) private var dismiss
    @State private var player: AVPlayer
    @State private var started = false
    @State private var failed = false

    init(file: TrailerFile) {
        self.file = file
        _player = State(initialValue: AVPlayer(url: file.url))
    }

    var body: some View {
        NativeTrailerPlayer(player: player)
            .ignoresSafeArea()
            .overlay {
                if !started && !failed { ProgressView("Starting Trailer").allowsHitTesting(false) }
            }
            .onExitCommand { dismiss() }
            .onDisappear {
                player.pause()
                player.replaceCurrentItem(with: nil)
            }
            .task {
                guard let item = player.currentItem else { return }
                player.play()
                let clock = ContinuousClock()
                let deadline = clock.now + .seconds(12)
                while !Task.isCancelled && !failed {
                    if item.status == .failed || (!started && clock.now >= deadline) {
                        player.pause()
                        failed = true
                        return
                    }
                    if !started && item.status == .readyToPlay && player.currentTime().seconds > 0.15 {
                        started = true
                    }
                    do { try await Task.sleep(for: .milliseconds(200)) }
                    catch { return }
                }
            }
            .onReceive(NotificationCenter.default.publisher(for: .AVPlayerItemDidPlayToEndTime, object: player.currentItem)) { _ in
                if player.currentItem != nil { dismiss() }
            }
            .onReceive(NotificationCenter.default.publisher(for: .AVPlayerItemFailedToPlayToEndTime, object: player.currentItem)) { _ in
                if player.currentItem != nil { failed = true; player.pause() }
            }
            .alert("Trailer could not play", isPresented: $failed) {
                Button("Close") { dismiss() }
            }
            #if PODIBLE_TRAILER_TEST_FIXTURE && targetEnvironment(simulator)
            .overlay(alignment: .topLeading) {
                if started && ProcessInfo.processInfo.environment["PODIBLE_TEST_TRAILER_URL"] != nil {
                    Text("Trailer playback started").accessibilityIdentifier("trailer.started")
                        .allowsHitTesting(false)
                }
            }
            #endif
    }
}

private struct NativeTrailerPlayer: UIViewControllerRepresentable {
    let player: AVPlayer

    func makeUIViewController(context: Context) -> AVPlayerViewController {
        let controller = AVPlayerViewController()
        controller.player = player
        controller.allowsPictureInPicturePlayback = false
        return controller
    }

    func updateUIViewController(_ controller: AVPlayerViewController, context: Context) {}
}

extension Connection {
    static func loadForTV() -> Self? {
        if ProcessInfo.processInfo.arguments.contains("--demo") { return nil }
        #if PODIBLE_TRAILER_TEST_FIXTURE && targetEnvironment(simulator)
        let env = ProcessInfo.processInfo.environment
        if let url = env["PODIBLE_TEST_TRAILER_URL"], let token = env["PODIBLE_TEST_TRAILER_TOKEN"],
           URL(string: url)?.host == "localhost" {
            return try? Self(server: url, token: token)
        }
        #endif
        return load()
    }
}
