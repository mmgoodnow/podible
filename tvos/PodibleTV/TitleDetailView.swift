import AVFoundation
import Foundation
import SwiftUI

struct TitleDetailView: View {
    let item: MediaTitle
    let seasons: [MediaRequestOption]
    let progress: TitleAcquisitionProgress?
    let play: (URL) -> Void
    let request: (Set<MediaRequestOption.ID>) async throws -> Void

    var body: some View {
        Content(item: item, seasons: seasons, progress: progress, play: play, request: request)
            .id(item.id)
    }
}

private extension TitleDetailView {
    struct Content: View {
        let item: MediaTitle
        let seasons: [MediaRequestOption]
        let progress: TitleAcquisitionProgress?
        let play: (URL) -> Void
        let request: (Set<MediaRequestOption.ID>) async throws -> Void

        @State private var selection: Set<MediaRequestOption.ID> = []
        @State private var submittedOptions: Set<MediaRequestOption.ID> = []
        @State private var submittedMovie = false
        @State private var confirmingMovie = false
        @State private var submitting = false
        @State private var requestError: String?
        @State private var preparingPlayback: ViewerPlaybackTarget?
        @State private var presentedPlayback: PlaybackPresentation?
        @State private var lastPlaybackTarget: ViewerPlaybackTarget?
        @State private var playbackError: String?
        @State private var viewerEpisodes: ViewerShowEpisodes?
        @State private var episodeError: String?
        @State private var episodeRetry = 0
        @FocusState private var focusedOption: MediaRequestOption.ID?
        @FocusState private var focusedPlayback: ViewerPlaybackTarget?

        @Environment(\.trailerConnection) private var trailerConnection
        private let base = Color(red: 0.045, green: 0.055, blue: 0.07)

        private var isMovie: Bool { item.kind == .movie }
        private var playbackURL: URL? { item.playable ? item.playbackURL : nil }
        private var moviePlayback: ViewerPlaybackTarget? {
            guard isMovie, item.requestState == .available, trailerConnection != nil,
                  let id = item.tmdbID, id > 0 else { return nil }
            return .movie(id)
        }

        private struct EpisodeLookup: Equatable {
            let connection: Connection
            let source: ViewerShowSource
            let coverage: [String]
            let retry: Int
        }

        private var episodeLookup: EpisodeLookup? {
            guard !isMovie, item.requestState == .available, let connection = trailerConnection,
                  let source = ViewerShowSource(item) else { return nil }
            return EpisodeLookup(connection: connection, source: source, coverage: seasons.map(\.summary), retry: episodeRetry)
        }

        private struct PlaybackPresentation: Identifiable {
            let grant: ViewerPlaybackGrant
            let asset: AVURLAsset
            let client: PodibleClient
            var id: String { grant.id }
        }
        private var canRequestMovie: Bool {
            isMovie && item.requestState == .requestable && !submittedMovie
        }

        private var eligibleOptions: Set<MediaRequestOption.ID> {
            Set(seasons.filter(\.requestable).map(\.id)).subtracting(submittedOptions)
        }

        private var selectedOptions: Set<MediaRequestOption.ID> { selection.intersection(eligibleOptions) }

        private var accent: Color {
            let raw = item.accent.trimmingCharacters(in: .whitespacesAndNewlines)
            let hex = raw.hasPrefix("#") ? String(raw.dropFirst()) : raw
            let rgb = hex.count == 3 ? hex.map { String(repeating: String($0), count: 2) }.joined() : hex
            guard rgb.count == 6, let value = UInt32(rgb, radix: 16) else {
                return Color(red: 0.94, green: 0.66, blue: 0.34)
            }
            return Color(
                red: Double((value >> 16) & 255) / 255,
                green: Double((value >> 8) & 255) / 255,
                blue: Double(value & 255) / 255
            )
        }

        var body: some View {
            GeometryReader { geometry in
                let heroHeight = isMovie ? max(580, geometry.size.height * 0.78) : 490

                ScrollView {
                    VStack(alignment: .leading, spacing: 42) {
                        hero
                            .padding(.horizontal, max(geometry.safeAreaInsets.leading, geometry.safeAreaInsets.trailing))
                            .padding(.top, geometry.safeAreaInsets.top)
                            .frame(minHeight: heroHeight, alignment: .bottomLeading)
                            .background(alignment: .top) {
                                GeometryReader { artwork in
                                    backdrop(width: artwork.size.width, height: artwork.size.height)
                                }
                            }

                        if let progress {
                            TitleRequestTracker(progress: progress)
                                .padding(.horizontal, 56 + max(geometry.safeAreaInsets.leading, geometry.safeAreaInsets.trailing))
                        }

                        if !isMovie {
                            if episodeLookup != nil {
                                episodeSection.padding(.horizontal, 56 + max(geometry.safeAreaInsets.leading, geometry.safeAreaInsets.trailing))
                            }
                            seasonSection
                                .padding(.horizontal, 56 + max(geometry.safeAreaInsets.leading, geometry.safeAreaInsets.trailing))
                        }
                    }
                    .padding(.bottom, 48)
                }
                .ignoresSafeArea(edges: [.top, .horizontal])
                .scrollIndicators(.hidden)
            }
            .background(base.ignoresSafeArea())
            .foregroundStyle(.white)
            .environment(\.colorScheme, .dark)
            .onChange(of: eligibleOptions) { _, eligible in
                selection.formIntersection(eligible)
            }
            .alert("Request Movie?", isPresented: $confirmingMovie) {
                Button("Cancel", role: .cancel) {}
                Button("Request") { submit([]) }
            } message: {
                Text("Send a request for \(item.title)?")
            }
            .task(id: episodeLookup) { await loadViewerEpisodes() }
            .task(id: preparingPlayback) { await prepareViewerPlayback() }
            .fullScreenCover(item: $presentedPlayback, onDismiss: { focusedPlayback = lastPlaybackTarget }) { playback in
                ViewerPlayback(asset: playback.asset)
                    .onDisappear {
                        Task { await playback.client.closeViewerPlayback(playback.id) }
                    }
            }
        }

        private var hero: some View {
            HStack(alignment: .bottom, spacing: 48) {
                poster

                VStack(alignment: .leading, spacing: 18) {
                    Label(isMovie ? "MOVIE" : "TV SERIES", systemImage: isMovie ? "film" : "tv")
                        .font(.system(size: 20, weight: .bold, design: .monospaced))
                        .tracking(3)
                        .foregroundStyle(Color.white.opacity(0.85))
                        .padding(.leading, 13)
                        .background(alignment: .leading) {
                            Capsule().fill(accent).frame(width: 4)
                        }

                    TVTitleTreatment(item: item)
                        .fixedSize(horizontal: false, vertical: true)

                    if !item.subtitle.isEmpty {
                        Text(item.subtitle)
                            .font(.system(size: 28, weight: .semibold))
                            .foregroundStyle(Color.white.opacity(0.85))
                    }

                    if !item.synopsis.isEmpty {
                        Text(item.synopsis)
                            .font(.system(size: 28))
                            .foregroundStyle(Color.white.opacity(0.8))
                            .fixedSize(horizontal: false, vertical: true)
                    }

                    HStack(spacing: 22) {
                        if let playbackURL {
                            Button { play(playbackURL) } label: {
                                Label(item.tmdbID == nil ? "Play Sample" : "Play", systemImage: "play.fill")
                            }
                            .buttonStyle(.borderedProminent)
                        }

                        if let moviePlayback {
                            viewerPlayButton(moviePlayback, title: "Play", accessibilityID: "playback.movie")
                        }

                        if canRequestMovie {
                            Button { confirmingMovie = true } label: {
                                Label("Request Movie", systemImage: "plus")
                            }
                            .buttonStyle(.bordered)
                            .disabled(submitting)
                        }

                        if !isMovie && progress == nil && item.requestState == .requested {
                            status("Requested", icon: "checkmark.circle")
                        } else if playbackURL == nil && moviePlayback == nil && item.requestState == .available {
                            status("In Library", icon: "checkmark.circle")
                        }

                        if let trailerConnection, let tmdbID = item.tmdbID, tmdbID > 0 {
                            TrailerButton(connection: trailerConnection, kind: item.kind, tmdbID: tmdbID,
                                          preferFocus: playbackURL == nil && moviePlayback == nil && episodeLookup == nil &&
                                            !canRequestMovie && !seasons.contains(where: \.requestable))
                            .id(tmdbID)
                        }
                    }
                    .focusSection()

                    if isMovie {
                        playbackFeedback
                        requestFeedback
                    }
                }
                .frame(maxWidth: 1050, alignment: .leading)
            }
            .padding(.horizontal, 56)
            .padding(.top, 60)
            .padding(.bottom, 12)
            .frame(maxWidth: .infinity, alignment: .leading)
        }

        private var episodeSection: some View {
            LazyVStack(alignment: .leading, spacing: 16) {
                Text("Episodes")
                    .font(.system(size: 44, weight: .bold))
                    .accessibilityAddTraits(.isHeader)
                if let episodes = viewerEpisodes {
                    if episodes.episodes.isEmpty {
                        Text("No published episodes with an exact Plex match are available to this viewer.")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(episodes.episodes) { episode in
                        if episode.playable {
                            viewerPlayButton(.episode(entryId: episodes.entryId, episodeId: episode.id),
                                             title: episode.label, accessibilityID: "playback.episode.\(episode.id)")
                        } else {
                            Text("\(episode.label) (open in Plex)").foregroundStyle(.secondary)
                        }
                    }
                } else if let episodeError {
                    Text(episodeError)
                    Button("Reload Episodes") { episodeRetry += 1 }.buttonStyle(.bordered)
                } else {
                    ProgressView("Checking Plex episodes")
                }
                playbackFeedback
            }
            .focusSection()
        }

        private func viewerPlayButton(_ target: ViewerPlaybackTarget, title: String, accessibilityID: String) -> some View {
            Button {
                playbackError = nil
                preparingPlayback = target
            } label: {
                Label(preparingPlayback == target ? "Preparing playback..." : title, systemImage: "play.fill")
            }
            .buttonStyle(.borderedProminent)
            .focused($focusedPlayback, equals: target)
            .disabled(preparingPlayback != nil)
            .accessibilityIdentifier(accessibilityID)
        }

        @ViewBuilder private var playbackFeedback: some View {
            if let playbackError {
                status(playbackError, icon: "exclamationmark.circle")
            }
        }

        private func loadViewerEpisodes() async {
            episodeError = nil
            viewerEpisodes = nil
            guard let lookup = episodeLookup else { return }
            let client = PodibleClient(baseURL: lookup.connection.url, token: lookup.connection.token)
            do {
                let episodes = try await client.viewerEpisodes(lookup.source)
                guard !Task.isCancelled, episodeLookup == lookup else { return }
                viewerEpisodes = episodes
            } catch {
                guard !Task.isCancelled, episodeLookup == lookup else { return }
                episodeError = viewerPlaybackMessage(error)
            }
        }

        private func prepareViewerPlayback() async {
            guard let target = preparingPlayback, let connection = trailerConnection else { return }
            defer { if !Task.isCancelled { preparingPlayback = nil } }
            let client = PodibleClient(baseURL: connection.url, token: connection.token)
            do {
                let grant = try await client.prepareViewerPlayback(target)
                guard !Task.isCancelled, trailerConnection == connection else {
                    Task { await client.closeViewerPlayback(grant.id) }
                    return
                }
                do {
                    let asset = try grant.asset(for: connection)
                    lastPlaybackTarget = target
                    presentedPlayback = PlaybackPresentation(grant: grant, asset: asset, client: client)
                } catch {
                    Task { await client.closeViewerPlayback(grant.id) }
                    throw error
                }
            } catch {
                guard !Task.isCancelled, trailerConnection == connection else { return }
                focusedPlayback = target
                playbackError = viewerPlaybackMessage(error)
            }
        }

        private func backdrop(width: CGFloat, height: CGFloat) -> some View {
            ZStack {
                base
                RadialGradient(
                    colors: [accent.opacity(0.45), base],
                    center: .topTrailing, startRadius: 20, endRadius: width * 0.7
                )
                TVBackdrop(item: item, connection: trailerConnection)
                    .frame(width: width, height: height)
                LinearGradient(
                    colors: [base.opacity(0.96), base.opacity(0.69), base.opacity(0.12)],
                    startPoint: .leading, endPoint: .trailing
                )
                LinearGradient(
                    stops: [
                        .init(color: base.opacity(0.12), location: 0),
                        .init(color: base.opacity(0.48), location: 0.42),
                        .init(color: base, location: 1)
                    ],
                    startPoint: .top, endPoint: .bottom
                )
            }
            .frame(width: width, height: height)
            .accessibilityHidden(true)
        }

        private var poster: some View {
            TVArtwork(url: item.artwork, title: item.title)
            .frame(width: 270, height: 405)
            .clipShape(.rect(cornerRadius: 18))
            .overlay {
                RoundedRectangle(cornerRadius: 18).strokeBorder(.white.opacity(0.22))
            }
            .shadow(color: .black.opacity(0.65), radius: 30, y: 16)
            .accessibilityHidden(true)
        }

        private var seasonSection: some View {
            let firstSpecial = seasons.first { $0.id.specialKey != nil }?.id
            return VStack(alignment: .leading, spacing: 20) {
                if seasons.isEmpty || seasons.contains(where: { $0.id.seasonNumber != nil }) {
                    Text("Seasons")
                        .font(.system(size: 44, weight: .bold))
                        .accessibilityAddTraits(.isHeader)
                }

                if seasons.isEmpty {
                    Text("No season information available.")
                        .foregroundStyle(.white.opacity(0.75))
                }

                ForEach(seasons) { season in
                    if season.id == firstSpecial {
                        Text("Specials")
                            .font(.system(size: 44, weight: .bold))
                            .accessibilityAddTraits(.isHeader)
                    }
                    if eligibleOptions.contains(season.id) {
                        let checked = selectedOptions.contains(season.id)
                        Button {
                            toggleOption(season.id)
                        } label: {
                            seasonLabel(season, icon: checked ? "checkmark.square.fill" : "square")
                                .padding(.horizontal, 24)
                                .padding(.vertical, 16)
                                .background(.white.opacity(focusedOption == season.id ? 0.12 : 0.06),
                                            in: .rect(cornerRadius: 16))
                                .overlay {
                                    RoundedRectangle(cornerRadius: 16)
                                        .strokeBorder(focusedOption == season.id ? TVPalette.accent : .clear,
                                                      lineWidth: 3)
                                }
                        }
                        .buttonStyle(TVUnstyledButton())
                        .focusEffectDisabled()
                        .focused($focusedOption, equals: season.id)
                        .disabled(submitting)
                        .accessibilityIdentifier(season.id.accessibilityIdentifier)
                        .accessibilityValue(checked ? "Selected" : "Not selected")
                    } else {
                        let sent = submittedOptions.contains(season.id)
                        seasonLabel(season, icon: sent ? "checkmark.circle" : "minus.circle")
                            .padding(.horizontal, 24)
                            .padding(.vertical, 16)
                            .background(.white.opacity(0.06), in: .rect(cornerRadius: 16))
                            .foregroundStyle(.white.opacity(0.75))
                            .accessibilityElement(children: .combine)
                    }
                }

                if !eligibleOptions.isEmpty {
                    HStack(spacing: 20) {
                        Button { submit(selectedOptions) } label: {
                            Label(firstSpecial == nil ? "Request Selected Seasons" : "Request Selected", systemImage: "plus")
                        }
                        .buttonStyle(.bordered)
                        .disabled(submitting || selectedOptions.isEmpty)
                        .accessibilityIdentifier("season.request")

                        Text(selectedOptions.isEmpty
                             ? (firstSpecial == nil ? "Select seasons to request." : "Select seasons or specials to request.")
                             : "\(selectedOptions.count) selected")
                            .foregroundStyle(.white.opacity(0.75))
                    }
                }

                requestFeedback
            }
            .focusSection()
        }

        private func seasonLabel(_ season: MediaRequestOption, icon: String) -> some View {
            HStack(spacing: 18) {
                Image(systemName: icon)
                    .font(.system(size: 29))
                    .frame(width: 40)
                    .accessibilityHidden(true)

                VStack(alignment: .leading, spacing: 4) {
                    Text(season.name).font(.system(size: 29, weight: .semibold))
                    if !season.summary.isEmpty {
                        Text(season.summary).font(.system(size: 23))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)

                if submittedOptions.contains(season.id) && season.summary != "Requested" {
                    Text("Requested")
                } else if !season.requestable && season.summary.isEmpty {
                    Text("Not requestable")
                }
            }
            .multilineTextAlignment(.leading)
        }

        @ViewBuilder private var requestFeedback: some View {
            if submitting {
                HStack(spacing: 14) {
                    ProgressView().accessibilityHidden(true)
                    Text("Sending request...")
                }
                .font(.system(size: 24))
            } else if let requestError {
                status("Request failed: \(requestError)", icon: "exclamationmark.circle")
            } else if !isMovie && !submittedOptions.isEmpty {
                status("Request sent", icon: "checkmark.circle.fill")
            }
        }

        private func status(_ title: String, icon: String) -> some View {
            Label(title, systemImage: icon)
                .font(.system(size: 24, weight: .semibold))
                .foregroundStyle(Color.white.opacity(0.85))
        }

        private func toggleOption(_ id: MediaRequestOption.ID) {
            guard !submitting, eligibleOptions.contains(id) else { return }
            requestError = nil
            if !selection.insert(id).inserted {
                selection.remove(id)
            }
        }

        private func submit(_ ids: Set<MediaRequestOption.ID>) {
            guard !submitting else { return }
            let movie = isMovie
            if movie {
                guard canRequestMovie else { return }
            } else {
                guard !ids.isEmpty, ids.isSubset(of: eligibleOptions) else { return }
            }

            submitting = true
            requestError = nil
            Task { @MainActor in
                defer { submitting = false }
                do {
                    try await request(ids)
                    if movie {
                        submittedMovie = true
                    } else {
                        submittedOptions.formUnion(ids)
                        selection.subtract(ids)
                    }
                } catch {
                    requestError = error.localizedDescription
                }
            }
        }
    }
}

private struct TitleRequestTracker: View {
    let progress: TitleAcquisitionProgress
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.trailerConnection) private var connection
    @State private var live: LiveSample?

    private struct StreamID: Equatable {
        let connection: Connection
        let media: TransferScope.Media
        let id: Int
    }

    private struct LiveSample {
        let stream: StreamID
        let sample: TransferEvent.SampleValue
        let received: ContinuousClock.Instant
    }

    private var streamID: StreamID? {
        guard scenePhase == .active, let connection, let scope = progress.transferScope else { return nil }
        return .init(connection: connection, media: scope.media, id: scope.id)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack(alignment: .firstTextBaseline) {
                Text(progress.status)
                    .font(.system(size: 31, weight: .semibold))
                    .accessibilityAddTraits(.isHeader)
                    .accessibilityIdentifier("request.progress.status")
                Spacer()
                if let episodeCount = progress.episodeCount {
                    Text(episodeCount).foregroundStyle(.secondary)
                }
            }
            let steps = TitleAcquisitionProgress.Step.allCases
            let current = steps.firstIndex(of: progress.step) ?? 0
            HStack(spacing: 10) {
                ForEach(steps.indices, id: \.self) { index in
                    VStack(alignment: .leading, spacing: 9) {
                        if steps[index] == .download, let live, live.stream == streamID {
                            TransferSegment(sample: live.sample, received: live.received)
                        } else {
                            Capsule().fill(index <= current ? TVPalette.accent : .white.opacity(0.18))
                                .frame(height: 5)
                            Text(steps[index].rawValue)
                                .font(.system(size: 21, weight: index == current ? .semibold : .regular))
                                .foregroundStyle(index <= current ? TVPalette.paper : .secondary)
                                .accessibilityAddTraits(index == current ? .isSelected : [])
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
        }
        .padding(26)
        .background(.white.opacity(0.07), in: RoundedRectangle(cornerRadius: 16))
        .task(id: streamID) {
            live = nil
            guard let id = streamID else { return }
            let client = PodibleClient(baseURL: id.connection.url, token: id.connection.token)
            do {
                try await AcquisitionChanges(client: client).observeTransfer(.init(media: id.media, id: id.id)) { sample in
                    guard !Task.isCancelled, streamID == id else { return }
                    live = sample.map { .init(stream: id, sample: $0, received: .now) }
                }
            } catch {
                if !Task.isCancelled, streamID == id { live = nil }
            }
        }
    }
}

private struct TransferSegment: View {
    let sample: TransferEvent.SampleValue
    let received: ContinuousClock.Instant
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @State private var projecting = false

    private var fraction: Double { min(0.999, Double(sample.bytesDone) / Double(sample.sizeBytes)) }
    private var projected: Double { min(0.999, (Double(sample.bytesDone) + acquisitionTransferIntervalSeconds * Double(sample.bytesPerSecond)) / Double(sample.sizeBytes)) }

    var body: some View {
        Capsule().fill(.white.opacity(0.18))
            .overlay(alignment: .leading) {
                GeometryReader { geometry in
                    Capsule().fill(TVPalette.accent)
                        .frame(width: geometry.size.width * (projecting ? projected : fraction))
                }
            }
            .frame(height: 5)
            .accessibilityHidden(true)
            .id(received)
            .task(id: received) {
                projecting = false
                guard !reduceMotion, sample.bytesPerSecond > 0 else { return }
                try? await Task.sleep(for: .milliseconds(25))
                guard !Task.isCancelled else { return }
                withAnimation(.linear(duration: acquisitionTransferIntervalSeconds - 0.025)) { projecting = true }
            }
        Text("Download (\(transferLabel))")
            .font(.system(size: 21, weight: .semibold))
            .foregroundStyle(TVPalette.paper)
            .accessibilityAddTraits(.isSelected)
            .accessibilityIdentifier("request.progress.download")
    }

    private var transferLabel: String {
        let format = ByteCountFormatter()
        format.countStyle = .binary
        var parts = ["\(Int(fraction * 100))%", format.string(fromByteCount: Int64(sample.sizeBytes))]
        if sample.bytesPerSecond > 0 { parts.append("\(format.string(fromByteCount: Int64(sample.bytesPerSecond)))/s") }
        if let eta = sample.etaSeconds, eta > 0 {
            let time = DateComponentsFormatter()
            time.unitsStyle = .abbreviated
            time.allowedUnits = [.hour, .minute, .second]
            time.maximumUnitCount = 2
            if let remaining = time.string(from: Double(eta)) { parts.append("ETA \(remaining)") }
        }
        return parts.joined(separator: " · ")
    }
}
