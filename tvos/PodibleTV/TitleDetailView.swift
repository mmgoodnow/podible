import Foundation
import SwiftUI

struct TitleDetailView: View {
    let item: MediaTitle
    let seasons: [MediaSeason]
    let play: (URL) -> Void
    let request: ([Int]) async throws -> Void

    var body: some View {
        Content(item: item, seasons: seasons, play: play, request: request)
            .id(item.id)
    }
}

private extension TitleDetailView {
    struct Content: View {
        let item: MediaTitle
        let seasons: [MediaSeason]
        let play: (URL) -> Void
        let request: ([Int]) async throws -> Void

        @State private var selection: Set<Int> = []
        @State private var submittedSeasons: Set<Int> = []
        @State private var submittedMovie = false
        @State private var confirmingMovie = false
        @State private var submitting = false
        @State private var requestError: String?

        private let base = Color(red: 0.045, green: 0.055, blue: 0.07)

        private var isMovie: Bool { item.kind == .movie }
        private var playbackURL: URL? { item.playable ? item.playbackURL : nil }
        private var canRequestMovie: Bool {
            isMovie && item.requestState == .requestable && !submittedMovie
        }

        private var eligibleSeasons: Set<Int> {
            Set(seasons.filter(\.requestable).map(\.id)).subtracting(submittedSeasons)
        }

        private var selectedSeasons: Set<Int> { selection.intersection(eligibleSeasons) }

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
                let heroHeight = max(580, geometry.size.height * 0.78)

                ScrollView {
                    VStack(alignment: .leading, spacing: 42) {
                        hero
                            .frame(minHeight: heroHeight, alignment: .bottomLeading)
                            .background(alignment: .top) {
                                backdrop(width: geometry.size.width, height: heroHeight)
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
            .onChange(of: eligibleSeasons) { _, eligible in
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

                    Text(item.title)
                        .font(.system(size: 76, weight: .heavy))
                        .accessibilityAddTraits(.isHeader)
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
                    AsyncImage(url: url) { phase in
                        if let image = phase.image {
                            image.resizable().scaledToFill()
                                .frame(width: width, height: height, alignment: .top)
                                .clipped()
                        }
                    }
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
            AsyncImage(url: item.artwork) { phase in
                if let image = phase.image {
                    image.resizable().scaledToFill()
                } else {
                    ZStack {
                        LinearGradient(colors: [accent.opacity(0.58), base],
                                       startPoint: .topLeading, endPoint: .bottomTrailing)
                        Image(systemName: isMovie ? "film" : "tv")
                            .font(.system(size: 62, weight: .light))
                            .foregroundStyle(.white.opacity(0.75))
                    }
                }
            }
            .frame(width: 270, height: 405)
            .clipShape(.rect(cornerRadius: 18))
            .overlay {
                RoundedRectangle(cornerRadius: 18).strokeBorder(.white.opacity(0.22))
            }
            .shadow(color: .black.opacity(0.65), radius: 30, y: 16)
            .accessibilityHidden(true)
        }

        private var seasonSection: some View {
            VStack(alignment: .leading, spacing: 20) {
                Text("Seasons")
                    .font(.system(size: 44, weight: .bold))
                    .accessibilityAddTraits(.isHeader)

                if !eligibleSeasons.isEmpty {
                    HStack(spacing: 20) {
                        Button { submit(selectedSeasons.sorted()) } label: {
                            Label("Request Selected Seasons", systemImage: "plus")
                        }
                        .buttonStyle(.bordered)
                        .disabled(submitting || selectedSeasons.isEmpty)

                        Text(selectedSeasons.isEmpty
                             ? "Select seasons to request."
                             : "\(selectedSeasons.count) selected")
                            .foregroundStyle(.white.opacity(0.75))
                    }
                }

                requestFeedback

                if seasons.isEmpty {
                    Text("No season information available.")
                        .foregroundStyle(.white.opacity(0.75))
                }

                ForEach(seasons) { season in
                    if eligibleSeasons.contains(season.id) {
                        let checked = selectedSeasons.contains(season.id)
                        Button {
                            toggleSeason(season.id)
                        } label: {
                            seasonLabel(season, icon: checked ? "checkmark.square.fill" : "square")
                        }
                        .buttonStyle(.bordered)
                        .disabled(submitting)
                        .accessibilityValue(checked ? "Selected" : "Not selected")
                    } else {
                        let sent = submittedSeasons.contains(season.id)
                        seasonLabel(season, icon: sent ? "checkmark.circle" : "minus.circle")
                            .padding(.horizontal, 24)
                            .padding(.vertical, 16)
                            .background(.white.opacity(0.06), in: .rect(cornerRadius: 16))
                            .foregroundStyle(.white.opacity(0.75))
                            .accessibilityElement(children: .combine)
                    }
                }
            }
            .focusSection()
        }

        private func seasonLabel(_ season: MediaSeason, icon: String) -> some View {
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

                if submittedSeasons.contains(season.id) {
                    Text("Requested")
                } else if !season.requestable {
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
            } else if submittedMovie || !submittedSeasons.isEmpty {
                status("Request sent", icon: "checkmark.circle.fill")
            }
        }

        private func status(_ title: String, icon: String) -> some View {
            Label(title, systemImage: icon)
                .font(.system(size: 24, weight: .semibold))
                .foregroundStyle(Color.white.opacity(0.85))
        }

        private func toggleSeason(_ id: Int) {
            guard !submitting, eligibleSeasons.contains(id) else { return }
            requestError = nil
            if !selection.insert(id).inserted {
                selection.remove(id)
            }
        }

        private func submit(_ ids: [Int]) {
            guard !submitting else { return }
            let movie = isMovie
            if movie {
                guard canRequestMovie else { return }
            } else {
                guard !ids.isEmpty, Set(ids).isSubset(of: eligibleSeasons) else { return }
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
                        submittedSeasons.formUnion(ids)
                        selection.subtract(ids)
                    }
                } catch {
                    requestError = error.localizedDescription
                }
            }
        }
    }
}
