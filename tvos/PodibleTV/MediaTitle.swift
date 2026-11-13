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

struct MediaRequestOption: Identifiable {
    enum ID: Hashable {
        case season(Int)
        case special(String)

        var seasonNumber: Int? {
            if case let .season(number) = self { return number }
            return nil
        }

        var specialKey: String? {
            if case let .special(key) = self { return key }
            return nil
        }

        var accessibilityIdentifier: String {
            switch self {
            case .season(let number): "season.\(number)"
            case .special(let key): "special.\(key)"
            }
        }
    }

    let id: ID
    let name: String
    let summary: String
    var requestable: Bool
}

struct TitleAcquisitionProgress: Sendable {
    enum Step: String, CaseIterable, Sendable {
        case requested = "Requested", search = "Search", download = "Download", verify = "Verify", plex = "Plex"

        static func from(stage: String?) -> Self {
            switch stage {
            case "searching", "selecting", "waiting", "waiting-for-release", "replacing": .search
            case "downloading", "pack-pending": .download
            case "verifying", "publishing": .verify
            case "ready", "available": .plex
            default: .requested
            }
        }
    }

    let step: Step
    let status: String
    var episodeCount: String? = nil
    var transferScope: TransferScope? = nil

    static let requested = Self(step: .requested, status: "Request received")

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

    static let expandedItems: [MediaTitle] = items + [
        ("The Quiet Atlas", MediaKind.movie, 1), ("Winter Current", .movie, 4), ("Harbor Light", .movie, 5),
        ("Far from the Shore", .movie, 2), ("The Long Crossing", .movie, 0), ("June in the Hills", .movie, 3),
        ("The Northern Route", .show, 4), ("Low Water", .show, 0), ("The Orchard Files", .show, 3),
        ("Coastal Roads", .show, 2), ("Night Radio", .show, 1), ("Below the Surface", .show, 5),
    ].enumerated().map { index, item in
        MediaTitle(id: "rail-fixture-\(index)", title: item.0, subtitle: "Demo library", synopsis: "", kind: item.1,
                   artwork: image(named: "demo-\(item.2)"), backdrop: nil, accent: items[item.2].accent,
                   playable: false, playbackURL: nil, requestState: .requestable, tmdbID: nil)
    }

    static func requestOptions(for item: MediaTitle, requested: Set<MediaRequestOption.ID>) -> [MediaRequestOption] {
        guard item.kind == .show else { return [] }
        let count = item.id == "tide" ? 1 : item.id == "north" ? 3 : 2
        let seasons = (1...count).map { season in
            let pending = requested.contains(.season(season)) || item.id == "tide"
            return MediaRequestOption(id: .season(season), name: "Season \(season)", summary: pending ? "Requested" : "8 episodes", requestable: !pending)
        }
        guard item.id == "north" else { return seasons }
        return seasons + ["Behind the Scenes", "The First Crossing"].enumerated().map { index, name in
            let id = MediaRequestOption.ID.special("tvdb:\(8001 + index)")
            let pending = requested.contains(id)
            return MediaRequestOption(id: id, name: "Special \(index + 1): \(name)", summary: pending ? "Requested" : "", requestable: !pending)
        }
    }

    private static func title(_ id: String, _ name: String, _ metadata: String, _ synopsis: String,
                              _ kind: MediaKind, _ accent: String, _ state: RequestState) -> MediaTitle {
        let index = ["marrow", "after", "isle", "orchard", "north", "tide"].firstIndex(of: id)
        return MediaTitle(id: id, title: name, subtitle: metadata, synopsis: synopsis, kind: kind,
                          artwork: index.flatMap { image(named: "demo-\($0)") },
                          backdrop: id == "marrow" ? image(named: "marrow-hero") : nil,
                          accent: accent, playable: state == .available,
                          playbackURL: state == .available ? sample : nil, requestState: state, tmdbID: nil)
    }

    private static func image(named name: String) -> URL? {
        Bundle.main.url(forResource: name, withExtension: "jpg", subdirectory: "DemoArtwork")
            ?? Bundle.main.url(forResource: name, withExtension: "jpg")
    }
}
