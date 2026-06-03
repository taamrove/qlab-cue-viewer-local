// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "QlabCueViewer",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "QlabCueViewer", targets: ["QlabCueViewer"])
    ],
    targets: [
        .executableTarget(
            name: "QlabCueViewer",
            path: "Sources/QlabCueViewer"
        )
    ]
)
