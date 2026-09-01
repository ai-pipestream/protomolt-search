// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "ProtomoltSearchDeviceTests",
    platforms: [.iOS(.v15)],
    products: [
        .library(name: "ProtomoltSearch", targets: ["ProtomoltSearch"])
    ],
    targets: [
        .binaryTarget(
            name: "ProtomoltSearch",
            path: "../../../target/mobile/ProtomoltSearch.xcframework"
        ),
        .testTarget(
            name: "ProtomoltSearchDeviceTests",
            dependencies: ["ProtomoltSearch"]
        )
    ]
)
