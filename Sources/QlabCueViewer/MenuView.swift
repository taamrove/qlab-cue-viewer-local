import SwiftUI
import AppKit

struct MenuView: View {
    @ObservedObject var bridge: Bridge
    @ObservedObject var settings: SettingsStore
    // SwiftUI's blessed way to open the Settings scene from a MenuBarExtra,
    // works correctly even when the app is in .accessory activation policy.
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        Text("QLab Cue Viewer").font(.headline)

        Divider()

        Label(qlabLine, systemImage: dotSymbol(bridge.qlabState))
        Label(relayLine, systemImage: dotSymbol(bridge.relayState))
        Text("Channel: \(settings.relayChannel)")

        if let snap = bridge.lastSnapshot {
            Divider()
            if let n = snap.playheadNumber, !n.isEmpty {
                Text("Cue \(n)")
            }
            if let name = snap.playheadName ?? snap.activeName {
                Text(name).foregroundStyle(.secondary)
            }
        }

        if let when = bridge.lastPublishedAt {
            // Static timestamp instead of relative-auto-updating Text — the
            // auto-updater plays badly with MenuBarExtra's render loop.
            Text("Last update: \(staticTime(when))")
                .font(.caption)
                .foregroundStyle(.tertiary)
        }

        Divider()

        if bridge.isRunning {
            Button("Stop", action: bridge.stop)
            Button("Restart", action: bridge.restart)
        } else {
            Button("Start", action: bridge.start)
        }

        // SwiftUI's SettingsLink causes infinite render recursion inside a
        // MenuBarExtra(.menu) on macOS 14+; openSettings() doesn't, and is the
        // correct call for .accessory apps where NSApp.sendAction can't find
        // a responder for showSettingsWindow:.
        Button("Preferences…") {
            NSApp.activate(ignoringOtherApps: true)
            openSettings()
        }
        .keyboardShortcut(",")

        Divider()
        Button("Quit") { NSApplication.shared.terminate(nil) }
            .keyboardShortcut("q")
    }

    private var qlabLine: String {
        switch bridge.qlabState {
        case .on:         return "QLab — connected"
        case .connecting: return "QLab — connecting…"
        case .off:        return "QLab — offline"
        }
    }

    private var relayLine: String {
        switch bridge.relayState {
        case .on:         return "Relay — connected"
        case .connecting: return "Relay — connecting…"
        case .off:        return "Relay — offline"
        }
    }

    private func dotSymbol(_ s: Bridge.LinkState) -> String {
        switch s {
        case .on:         return "circle.fill"
        case .connecting: return "circle.dotted"
        case .off:        return "circle"
        }
    }

    private static let timeFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()
    private func staticTime(_ d: Date) -> String { Self.timeFormatter.string(from: d) }
}
