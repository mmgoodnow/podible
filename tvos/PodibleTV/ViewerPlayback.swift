import AVKit
import SwiftUI

nonisolated enum ViewerPlaybackTarget: Hashable, Encodable, Sendable {
    case movie(Int)
    case episode(entryId: Int, episodeId: Int)

    func encode(to encoder: Encoder) throws {
        switch self {
        case .movie(let tmdbId):
            try ViewerMovieTarget(kind: .movie, tmdbId: tmdbId).encode(to: encoder)
        case .episode(let entryId, let episodeId):
            try ViewerEpisodeTarget(kind: .episode, entryId: entryId, episodeId: episodeId).encode(to: encoder)
        }
    }
}

extension ViewerShowSource: Equatable {
    static func == (lhs: Self, rhs: Self) -> Bool { lhs.source == rhs.source && lhs.id == rhs.id }

    init?(_ item: MediaTitle) {
        let prefix = "show:entry:"
        if item.id.hasPrefix(prefix), let entryId = Int(item.id.dropFirst(prefix.count)), entryId > 0 {
            self.init(source: .entry, id: entryId)
        } else if let tmdbId = item.tmdbID, tmdbId > 0 {
            self.init(source: .tmdb, id: tmdbId)
        } else { return nil }
    }
}

extension ViewerShowEpisodes.EpisodesValue: Identifiable {
    var label: String {
        if let season, let episode {
            return season == 0 ? "Special \(episode): \(title)" : "S\(season) E\(episode): \(title)"
        }
        return title
    }
}

extension ViewerPlaybackGrant: Identifiable {
    func asset(for connection: Connection) throws -> AVURLAsset {
        guard UUID(uuidString: id)?.uuidString.lowercased() == id,
              path == "/api/playback/file/\(id).mp4",
              credential.utf8.count == 43,
              credential.utf8.allSatisfy({ (65...90).contains($0) || (97...122).contains($0) ||
                  (48...57).contains($0) || $0 == 45 || $0 == 95 }),
              (1...21_600).contains(expiresInSeconds),
              var origin = URLComponents(url: connection.url, resolvingAgainstBaseURL: false),
              origin.scheme?.lowercased() == "https", origin.host != nil, origin.user == nil, origin.password == nil,
              origin.query == nil, origin.fragment == nil else { throw ClientError.invalidResponse }
        origin.path = path
        guard let url = origin.url, let cookie = HTTPCookie(properties: [
            .originURL: url, .path: path, .name: "podible_playback", .value: credential, .secure: "TRUE",
            .expires: Date().addingTimeInterval(Double(expiresInSeconds)),
        ]) else { throw ClientError.invalidResponse }
        return AVURLAsset(url: url, options: [
            AVURLAssetHTTPCookiesKey: [cookie],
            AVURLAssetReferenceRestrictionsKey: AVAssetReferenceRestrictions.forbidAll.rawValue,
        ])
    }
}

extension PodibleClient {
    func viewerEpisodes(_ source: ViewerShowSource) async throws -> ViewerShowEpisodes {
        try await call("playback.showEpisodes", source)
    }

    func prepareViewerPlayback(_ target: ViewerPlaybackTarget) async throws -> ViewerPlaybackGrant {
        try await call("playback.prepare", target)
    }

    func closeViewerPlayback(_ id: String) async {
        let _: ViewerPlaybackCloseReceipt? = try? await call("playback.close", ViewerPlaybackClose(id: id))
    }
}

func viewerPlaybackMessage(_ error: Error) -> String {
    switch error as? ClientError {
    case .unauthorized: "Sign in again on this Apple TV."
    case .rpc(-32003): "This Apple TV session does not have Plex playback access. Pair again by signing in to Plex on your phone, or check your library share."
    case .rpc(-32044): "No exact Plex match is available for playback."
    case .rpc(-32015): "This edition does not support direct play here. Open it in Plex to play it."
    case .rpc(-32029): "Too many playback sessions. Close another player and try again."
    default: "Podible could not prepare Plex playback. Try again later."
    }
}

struct ViewerPlayback: View {
    let asset: AVURLAsset
    @Environment(\.dismiss) private var dismiss
    @State private var player = AVPlayer()
    @State private var started = false
    @State private var failed = false

    var body: some View {
        ViewerPlayerController(player: player)
            .ignoresSafeArea()
            .overlay {
                if !started && !failed { ProgressView("Starting playback").allowsHitTesting(false) }
            }
            .task {
                let item = AVPlayerItem(asset: asset)
                player.allowsExternalPlayback = false
                player.replaceCurrentItem(with: item)
                player.play()
                let clock = ContinuousClock()
                let deadline = clock.now + .seconds(20)
                while !Task.isCancelled {
                    if item.status == .failed || (!started && clock.now >= deadline) {
                        player.pause()
                        failed = true
                        return
                    }
                    if player.timeControlStatus == .playing { started = true }
                    do { try await Task.sleep(for: .milliseconds(250)) }
                    catch { return }
                }
            }
            .onExitCommand { dismiss() }
            .onDisappear { player.pause(); player.replaceCurrentItem(with: nil) }
            .onReceive(NotificationCenter.default.publisher(for: .AVPlayerItemDidPlayToEndTime)) { notice in
                if let item = notice.object as? AVPlayerItem, item === player.currentItem { dismiss() }
            }
            .onReceive(NotificationCenter.default.publisher(for: .AVPlayerItemFailedToPlayToEndTime)) { notice in
                if let item = notice.object as? AVPlayerItem, item === player.currentItem {
                    player.pause()
                    failed = true
                }
            }
            .alert("Playback unavailable", isPresented: $failed) {
                Button("Close") { dismiss() }
            } message: {
                Text("The stream stopped or is no longer authorized. Try again from the title page.")
            }
    }
}

private struct ViewerPlayerController: UIViewControllerRepresentable {
    let player: AVPlayer

    func makeUIViewController(context: Context) -> AVPlayerViewController {
        let controller = AVPlayerViewController()
        controller.player = player
        return controller
    }

    func updateUIViewController(_ controller: AVPlayerViewController, context: Context) {}
}
