// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "PodibleTVTransport",
    platforms: [.macOS(.v14)],
    targets: [
        .target(name: "PodibleTVTransport", path: "PodibleTV",
                exclude: ["BrowseView.swift", "Connection.swift", "DemoArtwork", "PodibleTVApp.swift", "TitleDetailView.swift"],
                sources: ["AcquisitionChanges.swift", "MediaTitle.swift", "PodibleClient.swift"]),
        .testTarget(name: "PodibleTVTransportTests", dependencies: ["PodibleTVTransport"], path: "TransportTests"),
    ]
)
