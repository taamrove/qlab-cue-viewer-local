import SwiftUI
import AppKit
import OSLog

private let log = Logger(subsystem: "as.trv.qlab-cue-viewer", category: "settings")

struct SettingsView: View {
    @ObservedObject var bridge: Bridge
    @ObservedObject var settings: SettingsStore

    var body: some View {
        VStack(spacing: 0) {
            TabView {
                qlabTab.tabItem { Label("QLab", systemImage: "music.note.list") }
                relayTab.tabItem { Label("Relay", systemImage: "antenna.radiowaves.left.and.right") }
                advancedTab.tabItem { Label("Advanced", systemImage: "slider.horizontal.3") }
            }
            .padding()

            Divider()

            // Apply button + live status — outside the Form so the Form's row
            // hit-testing can't swallow the click, and visible from any tab.
            HStack(spacing: 16) {
                statusBadge("QLab", state: bridge.qlabState)
                statusBadge("Relay", state: bridge.relayState)
                Spacer()
                Button(bridge.isRunning ? "Apply & Restart" : "Start") {
                    log.info("Apply tapped (isRunning=\(bridge.isRunning))")
                    // Force any focused TextField to commit its value before
                    // we read settings back out — otherwise the latest
                    // keystroke can lag the binding by one event.
                    NSApp.keyWindow?.makeFirstResponder(nil)
                    bridge.restart()
                }
                .keyboardShortcut(.defaultAction)
            }
            .padding()
        }
    }

    // ─── tabs ────────────────────────────────────────────────────────────────

    private var qlabTab: some View {
        Form {
            TextField("Host", text: $settings.qlabHost)
                .textFieldStyle(.roundedBorder)
            TextField("Port", value: $settings.qlabPort, format: .number.grouping(.never))
                .textFieldStyle(.roundedBorder)
            SecureField("Passcode (optional)", text: $settings.qlabPasscode)
                .textFieldStyle(.roundedBorder)
            Text("OSC over UDP. Default: 127.0.0.1 : 53000 — enable QLab → Workspace Settings → Network → \"Use OSC controls\".")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding()
    }

    private var relayTab: some View {
        Form {
            TextField("Relay URL", text: $settings.relayURL)
                .textFieldStyle(.roundedBorder)
            SecureField("Token", text: $settings.relayToken)
                .textFieldStyle(.roundedBorder)
            TextField("Channel", text: $settings.relayChannel)
                .textFieldStyle(.roundedBorder)
            Text("The web viewer uses the same token + channel to subscribe.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding()
    }

    private var advancedTab: some View {
        Form {
            Stepper(
                "Poll interval: \(settings.pollIntervalMs) ms",
                value: $settings.pollIntervalMs,
                in: 50...2000,
                step: 50
            )
            Toggle("Auto-connect on launch", isOn: $settings.autoConnect)
        }
        .padding()
    }

    // ─── status footer ───────────────────────────────────────────────────────

    private func statusBadge(_ label: String, state: Bridge.LinkState) -> some View {
        let (color, text): (Color, String) = {
            switch state {
            case .on:         return (.green,  "on")
            case .connecting: return (.orange, "…")
            case .off:        return (.red,    "off")
            }
        }()
        return HStack(spacing: 4) {
            Circle().fill(color).frame(width: 8, height: 8)
            Text("\(label): \(text)").font(.caption).foregroundStyle(.secondary)
        }
    }
}
