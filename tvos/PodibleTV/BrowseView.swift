import SwiftUI

struct BrowseView: View {
    let items: [MediaTitle]
    let open: (MediaTitle) -> Void

    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @FocusState private var focus: FocusTarget?

    private enum Rail: String, CaseIterable {
        case movies = "Movies"
        case shows = "TV Shows"

        func contains(_ item: MediaTitle) -> Bool {
            switch (self, item.kind) {
            case (.movies, .movie), (.shows, .show): true
            default: false
            }
        }
    }

    private enum FocusTarget: Hashable {
        case featured
        case card(String)
    }

    private var featured: MediaTitle? {
        items.first { $0.backdrop != nil && $0.playable }
            ?? items.first { $0.backdrop != nil }
            ?? items.first { $0.playable }
            ?? items.first
    }

    private var spotlight: MediaTitle? {
        if case let .card(id) = focus, let item = items.first(where: { $0.id == id }) {
            return item
        }
        return featured
    }

    var body: some View {
        GeometryReader { geometry in
            if let spotlight, let featured {
                let inset: CGFloat = 70
                let cardWidth = min(320, max(220, geometry.size.width * 0.175))
                let heroHeight = min(540, max(390, geometry.size.height * 0.57))

                ScrollView(.vertical, showsIndicators: false) {
                    VStack(alignment: .leading, spacing: 24) {
                        hero(spotlight, featured: featured, height: heroHeight, inset: inset)
                            .focusSection()

                        ForEach(Rail.allCases, id: \.self) { rail in
                            let titles = items.filter(rail.contains)
                            if !titles.isEmpty {
                                shelf(rail, titles: titles, cardWidth: cardWidth, inset: inset)
                                    .focusSection()
                            }
                        }
                    }
                    .padding(.bottom, 44)
                }
                .defaultFocus($focus, .featured)
            } else {
                VStack(spacing: 20) {
                    Image(systemName: "rectangle.stack")
                        .font(.system(size: 56, weight: .light))
                        .foregroundStyle(Palette.gold)
                        .accessibilityHidden(true)
                    Text("No titles yet")
                        .font(.system(size: 42, weight: .semibold, design: .serif))
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .foregroundStyle(Palette.cream)
        .background {
            LinearGradient(colors: [Palette.teal, Palette.midnight], startPoint: .topLeading, endPoint: .bottomTrailing)
                .ignoresSafeArea()
        }
    }

    private func hero(_ item: MediaTitle, featured: MediaTitle, height: CGFloat, inset: CGFloat) -> some View {
        ZStack(alignment: .bottomLeading) {
            ZStack {
                heroBackdrop(item)
                    .id(item.id)
                    .transition(.opacity)
            }
            .accessibilityHidden(true)
            .allowsHitTesting(false)
            .animation(reduceMotion ? nil : .easeInOut(duration: 0.24), value: item.id)

            VStack(alignment: .leading, spacing: 20) {
                HStack(spacing: 12) {
                    Capsule().fill(Palette.gold).frame(width: 30, height: 3)
                        .accessibilityHidden(true)
                    Text(item.id == featured.id ? "Featured" : kindLabel(item))
                        .font(.system(size: 20, weight: .semibold, design: .monospaced))
                        .tracking(4)
                        .textCase(.uppercase)
                }
                .foregroundStyle(Palette.gold)

                Text(item.title)
                    .font(.system(size: 76, weight: .semibold, design: .serif))
                    .lineLimit(2)
                    .minimumScaleFactor(0.62)
                    .accessibilityAddTraits(.isHeader)

                if !item.subtitle.isEmpty {
                    Text(item.subtitle)
                        .font(.system(size: 26, weight: .medium))
                        .lineLimit(1)
                        .foregroundStyle(Palette.cream.opacity(0.83))
                }

                if !item.synopsis.isEmpty {
                    Text(item.synopsis)
                        .font(.system(size: 25))
                        .lineLimit(2)
                        .foregroundStyle(Palette.cream.opacity(0.76))
                }

                // The hero button keeps one focus identity as rail previews change.
                Button { open(item) } label: {
                    Label("View Details", systemImage: "arrow.right")
                        .font(.system(size: 23, weight: .semibold))
                        .foregroundStyle(focus == .featured ? Palette.midnight : Palette.cream)
                        .padding(.horizontal, 26)
                        .padding(.vertical, 14)
                        .background(focus == .featured ? Palette.gold : Palette.teal, in: Capsule())
                        .overlay {
                            Capsule().strokeBorder(Palette.gold.opacity(focus == .featured ? 1 : 0.46))
                        }
                        .contentShape(Capsule())
                }
                .buttonStyle(.plain)
                .focused($focus, equals: .featured)
                .accessibilityLabel("View details for \(item.title)")
                .accessibilityValue(statusLabel(item))
            }
            .frame(maxWidth: 670, alignment: .leading)
            .padding(.horizontal, inset)
            .padding(.bottom, 22)
        }
        .frame(height: height)
        .frame(maxWidth: .infinity)
    }

    private func heroBackdrop(_ item: MediaTitle) -> some View {
        GeometryReader { geometry in
            let width = geometry.size.width
            let height = geometry.size.height

            ZStack(alignment: .trailing) {
                RadialGradient(
                    colors: [accentColor(item).opacity(0.45), Palette.midnight.opacity(0)],
                    center: UnitPoint(x: 0.8, y: 0.3), startRadius: 20, endRadius: width * 0.52
                )

                let hasBackdrop = item.backdrop != nil
                Artwork(backdrop: item.backdrop, poster: item.artwork, placeholder: .clear)
                    .frame(width: width * (hasBackdrop ? 0.79 : 0.48), height: height)
                    .mask {
                        LinearGradient(
                            stops: [.init(color: .clear, location: 0),
                                    .init(color: .black.opacity(0.62), location: 0.35),
                                    .init(color: .black, location: 0.70)],
                            startPoint: .leading, endPoint: .trailing
                        )
                    }

                LinearGradient(
                    stops: [.init(color: Palette.midnight.opacity(0.22), location: 0),
                            .init(color: .clear, location: 0.25),
                            .init(color: Palette.midnight.opacity(0.58), location: 0.79),
                            .init(color: Palette.midnight, location: 1)],
                    startPoint: .top, endPoint: .bottom
                )
                LinearGradient(
                    stops: [.init(color: Palette.midnight.opacity(0.6), location: 0),
                            .init(color: .clear, location: 0.60)],
                    startPoint: .leading, endPoint: .trailing
                )
            }
        }
    }

    private func shelf(_ rail: Rail, titles: [MediaTitle], cardWidth: CGFloat, inset: CGFloat) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            Text(rail.rawValue)
                .font(.system(size: 32, weight: .semibold))
                .accessibilityAddTraits(.isHeader)
                .padding(.horizontal, inset)

            ScrollView(.horizontal, showsIndicators: false) {
                LazyHStack(alignment: .top, spacing: 30) {
                    ForEach(titles) { item in
                        card(item, width: cardWidth)
                    }
                }
                .padding(.horizontal, inset)
                .padding(.top, 18)
                .padding(.bottom, 16)
            }
        }
    }

    private func card(_ item: MediaTitle, width: CGFloat) -> some View {
        let target = FocusTarget.card(item.id)
        let isFocused = focus == target
        let shape = RoundedRectangle(cornerRadius: 15)

        return Button { open(item) } label: {
            VStack(alignment: .leading, spacing: 10) {
                ZStack(alignment: .topTrailing) {
                    Artwork(
                        backdrop: item.backdrop,
                        poster: item.artwork,
                        placeholder: accentColor(item).opacity(0.35),
                        symbol: item.kind == .movie ? "film" : "tv"
                    )

                    if !item.playable, let symbol = statusSymbol(item) {
                        Image(systemName: symbol)
                            .font(.system(size: 17, weight: .semibold))
                            .foregroundStyle(Palette.gold)
                            .frame(width: 38, height: 38)
                            .background(Palette.midnight.opacity(0.92), in: Circle())
                            .padding(10)
                    }
                }
                .frame(width: width, height: width * 9 / 16)
                .clipShape(shape)
                .overlay {
                    shape.strokeBorder(isFocused ? Palette.gold : .white.opacity(0.12), lineWidth: isFocused ? 4 : 1)
                }
                .shadow(color: isFocused ? Palette.gold.opacity(0.35) : .clear, radius: 13)
                .scaleEffect(isFocused && !reduceMotion ? 1.065 : 1)
                .accessibilityHidden(true)

                Text(item.title)
                    .font(.system(size: 23, weight: isFocused ? .semibold : .medium))
                    .foregroundStyle(isFocused ? Palette.cream : Palette.cream.opacity(0.74))
                    .lineLimit(1)
            }
            .frame(width: width, alignment: .leading)
            .contentShape(Rectangle())
            .animation(reduceMotion ? nil : .easeOut(duration: 0.16), value: isFocused)
        }
        .buttonStyle(.plain)
        .focused($focus, equals: target)
        .accessibilityLabel(item.title)
        .accessibilityValue(statusLabel(item))
        .zIndex(isFocused ? 1 : 0)
    }

    private func kindLabel(_ item: MediaTitle) -> String {
        item.kind == .movie ? "Movie" : "TV Show"
    }

    private func statusLabel(_ item: MediaTitle) -> String {
        if item.playable { return "Ready to watch" }
        if item.requestState == .available { return "In library" }
        if item.requestState == .requestable { return "Can be requested" }
        if item.requestState == .requested { return "Requested" }
        return "Unavailable"
    }

    private func statusSymbol(_ item: MediaTitle) -> String? {
        if item.requestState == .requestable { return "plus" }
        if item.requestState == .requested { return "clock" }
        return nil
    }

    private func accentColor(_ item: MediaTitle) -> Color {
        let hex = item.accent.trimmingCharacters(in: .whitespacesAndNewlines)
        let digits = hex.hasPrefix("#") ? String(hex.dropFirst()) : hex
        guard digits.count == 6, let rgb = UInt32(digits, radix: 16) else { return Palette.teal }
        return Palette.color(rgb)
    }

    private enum Palette {
        static let midnight = color(0x061416)
        static let teal = color(0x103437)
        static let gold = color(0xF3C576)
        static let cream = color(0xFFF4DF)

        static func color(_ rgb: UInt32) -> Color {
            Color(red: Double((rgb >> 16) & 255) / 255,
                  green: Double((rgb >> 8) & 255) / 255,
                  blue: Double(rgb & 255) / 255)
        }
    }

    private struct Artwork: View {
        let backdrop: URL?
        let poster: URL?
        let placeholder: Color
        var symbol: String? = nil

        var body: some View {
            GeometryReader { geometry in
                artwork(size: geometry.size)
                    .frame(width: geometry.size.width, height: geometry.size.height)
                    .clipped()
            }
        }

        @ViewBuilder
        private func artwork(size: CGSize) -> some View {
            if let backdrop, backdrop != poster {
                AsyncImage(url: backdrop) { phase in
                    switch phase {
                    case .success(let image): fitted(image, size: size)
                    case .failure: posterImage(size: size)
                    default: fallback
                    }
                }
            } else {
                posterImage(size: size)
            }
        }

        @ViewBuilder
        private func posterImage(size: CGSize) -> some View {
            if let poster {
                AsyncImage(url: poster) { phase in
                    if let image = phase.image {
                        fitted(image, size: size)
                    } else {
                        fallback
                    }
                }
            } else {
                fallback
            }
        }

        private func fitted(_ image: Image, size: CGSize) -> some View {
            image.resizable().scaledToFill().frame(width: size.width, height: size.height)
        }

        private var fallback: some View {
            ZStack {
                if let symbol {
                    LinearGradient(colors: [placeholder, Palette.teal], startPoint: .topTrailing, endPoint: .bottomLeading)
                    Image(systemName: symbol)
                        .font(.system(size: 42, weight: .light))
                        .foregroundStyle(Palette.cream.opacity(0.45))
                } else {
                    placeholder
                }
            }
        }
    }
}
