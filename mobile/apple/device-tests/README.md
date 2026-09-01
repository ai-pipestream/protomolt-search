# Apple device tests

Build `target/mobile/ProtomoltSearch.xcframework` first, then open this package
in Xcode and run the `ProtomoltSearchDeviceTests` scheme on an iPhone and an
iPhone Simulator. The lifecycle test covers mapped ingest, query and query
stream, flush/background-style close and reopen, persistent disk size, and
socket creation. The performance test records XCTest CPU, clock, and logical
storage metrics; accept or change performance baselines only from repeated
physical-device runs on the same hardware and OS.
