# qlab-cue-viewer-local

Native macOS menu bar app. Connects to QLab 5's WebSocket API on the show machine, builds a cue-state snapshot, and publishes it to [`relay-server`](../relay-server/) so the [web viewer](../qlab-cue-viewer-web/) can pick it up from anywhere.

Built with SwiftPM + SwiftUI (`MenuBarExtra`). No Xcode project file — just `swift build` / `swift run`.

## Requirements

- macOS 14+
- Swift 5.10+ (ships with Xcode 15 / Command Line Tools 15)
- QLab 5 running locally (or reachable on the LAN)

## Dev

```sh
swift run                          # builds and launches
swift build -c release             # release binary at .build/release/QlabCueViewer
```

On first launch:

1. Click the menu bar icon → **Preferences…**
2. **QLab** tab — set URL (default `ws://127.0.0.1:53000`) and passcode if you set one in QLab → Settings → Network → OSC
3. **Relay** tab — paste the token from `../relay-server/.deployed-token`, pick a channel name (e.g. `qlab-show-1`)
4. Click **Start**

The menu bar icon goes solid when both QLab and the relay are connected. The menu shows the current playhead cue and timestamp of the last published snapshot.

## Project layout

```
Package.swift
Sources/QlabCueViewer/
├── QlabCueViewerApp.swift   # @main, MenuBarExtra scene
├── Bridge.swift             # coordinator: owns QLab + Relay, publishes state
├── QLabClient.swift         # WebSocket client → QLab 5 (polls v0)
├── RelayPublisher.swift     # WebSocket publisher → relay-server
├── Settings.swift           # UserDefaults-backed @AppStorage wrapper
├── MenuView.swift           # MenuBarExtra content
└── SettingsView.swift       # Preferences window (TabView)
scripts/discover.js          # Node QLab API discovery probe (zero deps)
```

## Discovery probe

Throwaway tool to learn QLab's API surface and shape the timeline data model:

```sh
QLAB_PASSCODE=… node scripts/discover.js
```

Writes timestamped `.json` + `.md` to `discovery-output/`. Read the `.md` first.

## Building a shareable `.app`

The `Makefile` wraps the SwiftPM build into a Finder-launchable `.app`:

```sh
make app          # current-arch .app (fast)
make universal    # universal (arm64 + x86_64) .app — works on any Mac
make run          # build + launch
make zip          # → dist/QlabCueViewer-<version>.zip
```

The `.app` is ad-hoc codesigned, so macOS will let the build run on the build machine without complaint. For someone else's Mac, see the next section.

## Installing on a different Mac

Hand them `dist/QlabCueViewer-<version>.zip` (AirDrop, Slack, USB stick, whatever).

On their Mac:

1. Unzip → drag `QlabCueViewer.app` to `/Applications`.
2. **First launch only**: right-click the app → **Open** → confirm "Open" in the Gatekeeper dialog. (macOS does this because the build isn't notarized with an Apple Developer ID. After confirming once, normal double-click works forever.)
3. The icon appears in the menu bar — click it → **Preferences…** and fill in:
   - **QLab** tab — Host (`127.0.0.1` if QLab runs on the same Mac), Port (`53000`), Passcode (only if QLab has one set in Workspace Settings → Network → OSC Access)
   - **Relay** tab — URL `wss://relay.trv.as`, Token (the relay's publisher token — from `relay-server/.deployed-token` on your dev machine), Channel name (pick a non-guessable string per show; subscribers connect to the same channel)
4. Click **Apply & Restart**. The QLab and Relay dots in the Preferences footer should both turn green within a couple of seconds.
5. In QLab → Workspace Settings → Network → OSC Access, make sure **"Allow OSC connections"** is checked.
