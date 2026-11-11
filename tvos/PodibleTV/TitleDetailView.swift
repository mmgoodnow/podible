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
        @FocusState private var focusedOption: MediaRequestOption.ID?

        @Environment(\.trailerConnection) private var trailerConnection
        private let base = Color(red: 0.045, green: 0.055, blue: 0.07)

        private var isMovie: Bool { item.kind == .movie }
        private var playbackURL: URL? { item.playable ? item.playbackURL : nil }
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
                            .frame(minHeight: heroHeight, alignment: .bottomLeading)
                            .background(alignment: .top) {
                                backdrop(width: geometry.size.width, height: heroHeight)
                            }

                        if let progress {
                            TitleRequestTracker(progress: progress)
                                .padding(.horizontal, 56)
                        }

                        if !isMovie {
                            seasonSection
                                .padding(.horizontal, 56)
                        }
                    }
                    .padding(.bottom, 48)
                }
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

                        if canRequestMovie {
                            Button { confirmingMovie = true } label: {
                                Label("Request Movie", systemImage: "plus")
                            }
                            .buttonStyle(.bordered)
                            .disabled(submitting)
                        }

                        if submittedMovie || item.requestState == .requested {
                            status("Requested", icon: "checkmark.circle")
                        } else if playbackURL == nil && item.requestState == .available {
                            status("In Library", icon: "checkmark.circle")
                        }

                        if let trailerConnection, let tmdbID = item.tmdbID, tmdbID > 0 {
                            TrailerButton(connection: trailerConnection, kind: item.kind, tmdbID: tmdbID,
                                          preferFocus: playbackURL == nil && !canRequestMovie && !seasons.contains(where: \.requestable))
                            .id(tmdbID)
                        }
                    }
                    .focusSection()

                    if isMovie {
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

        private func backdrop(width: CGFloat, height: CGFloat) -> some View {
            ZStack {
                base
                RadialGradient(
                    colors: [accent.opacity(0.45), base],
                    center: .topTrailing, startRadius: 20, endRadius: width * 0.7
                )
                if let url = item.backdrop {
                    TVArtwork(url: url, alignment: .top)
                        .frame(width: width, height: height)
                }
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
            .ignoresSafeArea(edges: [.top, .horizontal])
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
            } else if submittedMovie || !submittedOptions.isEmpty {
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
                        Capsule().fill(index <= current ? TVPalette.accent : .white.opacity(0.18))
                            .frame(height: 5)
                        Text(steps[index].rawValue)
                            .font(.system(size: 21, weight: index == current ? .semibold : .regular))
                            .foregroundStyle(index <= current ? TVPalette.paper : .secondary)
                            .accessibilityAddTraits(index == current ? .isSelected : [])
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            if let sample = progress.download, let done = sample.bytesDone, let size = sample.sizeBytes, size > 0 {
                ProgressView(value: Double(min(max(0, done), size)), total: Double(size))
                    .tint(TVPalette.accent)
                Text(transferLabel(sample, done: done, size: size))
                    .font(.system(size: 21))
                    .foregroundStyle(.secondary)
            }
        }
        .padding(26)
        .background(.white.opacity(0.07), in: RoundedRectangle(cornerRadius: 16))
    }

    private func transferLabel(_ sample: TitleAcquisitionProgress.Download, done: Int64, size: Int64) -> String {
        let format = ByteCountFormatter()
        format.countStyle = .binary
        var parts = ["\(format.string(fromByteCount: done)) of \(format.string(fromByteCount: size))"]
        if let speed = sample.bytesPerSecond, speed > 0 { parts.append("\(format.string(fromByteCount: speed))/s") }
        if let eta = sample.etaSeconds, eta > 0 {
            let time = DateComponentsFormatter()
            time.unitsStyle = .abbreviated
            time.allowedUnits = [.hour, .minute, .second]
            time.maximumUnitCount = 2
            if let remaining = time.string(from: Double(eta)) { parts.append("\(remaining) left") }
        }
        return parts.joined(separator: " · ")
    }
}
