import SwiftUI
import UIKit

enum TVPalette {
    static let background = Color(red: 0.043, green: 0.051, blue: 0.067)
    static let paper = Color(red: 0.97, green: 0.95, blue: 0.9)
    static let accent = Color(red: 0.97, green: 0.76, blue: 0.43)
}

struct BrowseView: View {
    let items: [MediaTitle]
    let focusRequest: Int
    let connection: Connection?
    let open: (MediaTitle) -> Void

    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @FocusState private var focusedID: String?
    @State private var previewID: String?
    private let railInset: CGFloat = 76
    private let posterSpacing: CGFloat = 23

    private var spotlight: MediaTitle? {
        items.first(where: { $0.id == previewID }) ?? items.first(where: { $0.kind == .movie }) ?? items.first
    }

    var body: some View {
        GeometryReader { geometry in
            if let spotlight {
                let heroHeight = min(455, max(320, geometry.size.height * 0.46))
                let compactCatalog = items.count <= 8
                let railWidth = geometry.size.width - railInset + geometry.safeAreaInsets.trailing
                let cardWidth = max(140, (railWidth + posterSpacing) / 6.6 - posterSpacing)
                VStack(alignment: .leading, spacing: 0) {
                    hero(spotlight, width: geometry.size.width, height: heroHeight, insets: geometry.safeAreaInsets)
                    ScrollView(.vertical) {
                        VStack(alignment: .leading, spacing: 13) {
                            if compactCatalog {
                                shelf("Movies & TV", titles: items, cardWidth: cardWidth)
                            } else {
                                shelf("Movies", titles: items.filter { $0.kind == .movie }, cardWidth: cardWidth)
                                shelf("TV Shows", titles: items.filter { $0.kind == .show }, cardWidth: cardWidth)
                            }
                        }
                        .padding(.top, 4)
                        .padding(.bottom, 48)
                    }
                    .scrollIndicators(.hidden)
                    .ignoresSafeArea(edges: .bottom)
                    .accessibilityIdentifier("browse.shelves")
                }
                .defaultFocus($focusedID, spotlight.id)
                .task(id: focusRequest) {
                    guard focusRequest > 0 else { return }
                    // Let a newly selected rail register its focus targets before moving out of the overlay.
                    try? await Task.sleep(for: .milliseconds(50))
                    guard !Task.isCancelled else { return }
                    focusedID = items.first(where: { $0.kind == .movie })?.id ?? items.first?.id
                }
                .onChange(of: focusedID) { _, id in
                    if let id { previewID = id }
                }
            }
        }
    }

    private func hero(_ item: MediaTitle, width: CGFloat, height: CGFloat, insets: EdgeInsets) -> some View {
        ZStack(alignment: .leading) {
            Color.clear.overlay(alignment: .topTrailing) {
                TVBackdrop(item: item, connection: connection, debounce: .milliseconds(180))
                    .frame(width: width * 0.81 + insets.trailing, height: height + insets.top)
                    .mask {
                        LinearGradient(stops: [
                            .init(color: .clear, location: 0),
                            .init(color: .black.opacity(0.35), location: 0.27),
                            .init(color: .black, location: 0.65)
                        ], startPoint: .leading, endPoint: .trailing)
                    }
                    .overlay(alignment: .bottom) {
                        LinearGradient(colors: [.clear, TVPalette.background], startPoint: .top, endPoint: .bottom)
                            .frame(height: height * 0.48)
                    }
                    .offset(x: insets.trailing, y: -insets.top)
                    .accessibilityHidden(true)
            }
            VStack(alignment: .leading, spacing: 13) {
                TVTitleTreatment(item: item)
                HStack(spacing: 16) {
                    if !item.subtitle.isEmpty { Text(item.subtitle) }
                    if item.kind == .show && item.requestState == .requested {
                        Label("Requested", systemImage: "clock")
                    } else if item.requestState == .available {
                        Label("In Library", systemImage: "checkmark.circle")
                    }
                }
                .font(.system(size: 23, weight: .medium))
                .foregroundStyle(TVPalette.paper.opacity(0.82))
                .lineLimit(1)
                if !item.synopsis.isEmpty {
                    Text(item.synopsis)
                        .font(.system(size: 25))
                        .foregroundStyle(TVPalette.paper.opacity(0.84))
                        .lineLimit(2)
                }
            }
            .frame(maxWidth: min(810, width * 0.5), alignment: .leading)
            .padding(.leading, railInset)
        }
        .id(item.id)
        .transition(.opacity)
        .frame(width: width, height: height)
        .animation(reduceMotion ? nil : .easeOut(duration: 0.18), value: item.id)
    }

    @ViewBuilder
    private func shelf(_ title: String, titles: [MediaTitle], cardWidth: CGFloat) -> some View {
        if !titles.isEmpty {
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.system(size: 30, weight: .semibold))
                    .accessibilityAddTraits(.isHeader)
                    .padding(.leading, railInset)
                ScrollView(.horizontal) {
                    LazyHStack(alignment: .top, spacing: posterSpacing) {
                        ForEach(titles) { item in
                            TVPosterButton(item: item, width: cardWidth, focused: focusedID == item.id) { open(item) }
                                .focused($focusedID, equals: item.id)
                        }
                    }
                    .padding(.trailing, 42)
                    .padding(.vertical, 9)
                }
                .scrollIndicators(.hidden)
                // Keep the focus viewport clear of the pill, but let unfocused artwork scroll behind it.
                .scrollClipDisabled()
                .padding(.leading, railInset)
                .ignoresSafeArea(edges: .trailing)
            }
            .focusSection()
        }
    }
}

struct TVTitleTreatment: View {
    let item: MediaTitle

    var body: some View {
        if item.tmdbID == nil && item.id == "after" {
            VStack(alignment: .leading, spacing: -7) {
                Text("AFTER THE").font(.system(size: 31, weight: .semibold, design: .monospaced)).tracking(7)
                Text("SIGNAL").font(.system(size: 82, weight: .heavy, design: .monospaced)).tracking(8)
            }
            .accessibilityElement(children: .ignore)
            .accessibilityLabel(item.title)
            .accessibilityAddTraits(.isHeader)
        } else {
            let demo = item.tmdbID == nil
            Text(demo && item.id == "north" ? item.title.uppercased() : item.title)
                .font(.system(size: 76, weight: demo && item.id == "marrow" ? .medium : .bold,
                              design: demo && ["marrow", "orchard"].contains(item.id) ? .serif : .default))
                .italic(demo && item.id == "tide")
                .tracking(demo && item.id == "marrow" ? 2 : -1)
                .lineLimit(2)
                .minimumScaleFactor(0.62)
                .accessibilityAddTraits(.isHeader)
        }
    }
}

struct TVPosterButton: View {
    let item: MediaTitle
    let width: CGFloat
    let focused: Bool
    let open: () -> Void
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        let shape = RoundedRectangle(cornerRadius: 11)
        Button(action: open) {
            VStack(alignment: .leading, spacing: 10) {
                TVArtwork(url: item.artwork, title: item.title)
                    .frame(width: width, height: width * 1.5)
                    .clipShape(shape)
                    .overlay {
                        shape.strokeBorder(focused ? TVPalette.accent : .white.opacity(0.14),
                                           lineWidth: focused ? 3 : 1)
                    }
                    .accessibilityHidden(true)
                Text(item.title)
                    .font(.system(size: 21, weight: focused ? .semibold : .medium))
                    .lineLimit(1)
                    .foregroundStyle(TVPalette.paper.opacity(focused ? 1 : 0.74))
            }
            .frame(width: width, alignment: .leading)
            .scaleEffect(focused && !reduceMotion ? 1.045 : 1)
            .animation(reduceMotion ? nil : .easeOut(duration: 0.15), value: focused)
        }
        .buttonStyle(TVUnstyledButton())
        .focusEffectDisabled()
        .accessibilityLabel(item.title)
        .accessibilityValue(item.requestState.label)
        .accessibilityIdentifier("poster.\(item.id)")
    }
}

struct TVUnstyledButton: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .opacity(configuration.isPressed ? 0.8 : 1)
    }
}

struct TVBackdrop: View {
    let item: MediaTitle
    let connection: Connection?
    var debounce: Duration = .zero

    @State private var resolved: Resolved?

    private struct Lookup: Equatable {
        let kind: MediaKind
        let tmdbID: Int
        let connection: Connection
    }

    private struct Resolved {
        let lookup: Lookup
        let url: URL?
    }

    private var lookup: Lookup? {
        guard item.backdrop == nil, let tmdbID = item.tmdbID, let connection else { return nil }
        return Lookup(kind: item.kind, tmdbID: tmdbID, connection: connection)
    }

    var body: some View {
        let lookup = lookup
        TVArtwork(url: item.backdrop ?? (resolved?.lookup == lookup ? resolved?.url : nil), ambientPoster: item.artwork)
            .task(id: lookup) {
                guard let lookup, resolved?.lookup != lookup else { return }
                do {
                    try await Task.sleep(for: debounce)
                    let client = PodibleClient(baseURL: lookup.connection.url, token: lookup.connection.token)
                    let url = try await client.backdrop(kind: lookup.kind, tmdbID: lookup.tmdbID)
                    try Task.checkCancellation()
                    resolved = Resolved(lookup: lookup, url: url)
                } catch {
                    // Optional artwork must not block browsing or playback.
                }
            }
    }
}

struct TVArtwork: View {
    let url: URL?
    var title: String? = nil
    var alignment: Alignment = .center
    var ambientPoster: URL? = nil

    var body: some View {
        GeometryReader { geometry in
            Group {
                if let url, url.isFileURL, let image = UIImage(contentsOfFile: url.path) {
                    Image(uiImage: image).resizable().scaledToFill()
                } else if let url {
                    AsyncImage(url: url) { phase in
                        if let image = phase.image {
                            image.resizable().scaledToFill()
                        } else {
                            fallback(in: geometry.size)
                        }
                    }
                } else {
                    fallback(in: geometry.size)
                }
            }
            .frame(width: geometry.size.width, height: geometry.size.height, alignment: alignment)
            .clipped()
        }
    }

    @ViewBuilder private func fallback(in size: CGSize) -> some View {
        if let ambientPoster {
            AmbientPoster(url: ambientPoster)
                .frame(width: size.width, height: size.height)
                .scaleEffect(1.18)
                .blur(radius: 64)
                .opacity(0.43)
        } else if let title {
            LinearGradient(colors: [.white.opacity(0.12), TVPalette.background],
                           startPoint: .topTrailing, endPoint: .bottomLeading)
                .overlay(alignment: .bottomLeading) {
                    Text(title)
                        .font(.system(size: 25, weight: .semibold, design: .serif))
                        .lineLimit(3)
                        .padding(16)
                }
        } else {
            Color.clear
        }
    }
}

private struct AmbientPoster: View {
    let url: URL

    var body: some View {
        if url.isFileURL, let image = UIImage(contentsOfFile: url.path) {
            Image(uiImage: image).resizable().scaledToFill()
        } else {
            AsyncImage(url: url) { phase in
                if let image = phase.image {
                    image.resizable().scaledToFill()
                } else {
                    Color.clear
                }
            }
        }
    }
}
