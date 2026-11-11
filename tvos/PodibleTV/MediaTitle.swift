import Foundation

enum MediaKind: String, Hashable {
    case movie, show
    var label: String { self == .movie ? "Movie" : "TV show" }
}

enum RequestState: Hashable {
    case available, requestable, requested, unavailable

    var label: String {
        switch self {
        case .available: "In library"
        case .requestable: "Available to request"
        case .requested: "Requested"
        case .unavailable: "Unavailable"
        }
    }
}

struct MediaTitle: Identifiable, Hashable {
    let id: String
    let title: String
    let subtitle: String
    let synopsis: String
    let kind: MediaKind
    let artwork: URL?
    let backdrop: URL?
    let accent: String
    let playable: Bool
    let playbackURL: URL?
    var requestState: RequestState
    let tmdbID: Int?
}

struct MediaSeason: Identifiable {
    let id: Int
    let name: String
    let summary: String
    var requestable: Bool
}

enum DemoCatalog {
    private static let sample = URL(string: "https://devstreaming-cdn.apple.com/videos/streaming/examples/img_bipbop_adv_example_ts/master.m3u8")

    static let items = [
        title("marrow", "The Marrow Sea", "2025 / 1h 54m / Adventure", "A cartographer follows a lighthouse signal out past the last marked island.", .movie, "#287A77", .available),
        title("after", "After the Signal", "2024 / 2 seasons / Mystery", "When a radio array starts picking up tomorrow's broadcasts, a mountain town has one night to decide what to believe.", .show, "#68664B", .requestable),
        title("isle", "Low Island", "2025 / 1h 42m / Drama", "Two sisters return to the island they left behind just as the last ferry is about to depart.", .movie, "#477F8F", .available),
        title("orchard", "An Orchard in Winter", "2023 / 1h 47m / Drama", "An unexpected inheritance brings four strangers together at a remote orchard.", .movie, "#A16048", .requestable),
        title("north", "Northbound", "2025 / 3 seasons / Adventure", "Three crews race winter across the edges of a changing coastline.", .show, "#54766C", .requestable),
        title("tide", "Tide Lines", "2025 / 1 season / Documentary", "A season on the water with the people restoring the North Atlantic kelp forests.", .show, "#465C8E", .requested),
    ]

    static func seasons(for item: MediaTitle, requested: Set<Int>) -> [MediaSeason] {
        guard item.kind == .show else { return [] }
        let count = item.id == "tide" ? 1 : item.id == "north" ? 3 : 2
        return (1...count).map { season in
            let pending = requested.contains(season) || item.id == "tide"
            return MediaSeason(id: season, name: "Season \(season)", summary: pending ? "Requested" : "8 episodes", requestable: !pending)
        }
    }

    private static func title(_ id: String, _ name: String, _ metadata: String, _ synopsis: String,
                              _ kind: MediaKind, _ accent: String, _ state: RequestState) -> MediaTitle {
        MediaTitle(id: id, title: name, subtitle: metadata, synopsis: synopsis, kind: kind,
                   artwork: nil, backdrop: nil, accent: accent, playable: state == .available,
                   playbackURL: state == .available ? sample : nil, requestState: state, tmdbID: nil)
    }
}
