import SwiftUI

@main
struct QlabCueViewerApp: App {
    @StateObject private var bridge = Bridge()

    var body: some Scene {
        // Menu bar item. Icon reflects combined connection state; menu shows
        // detail + "Preferences…" + "Quit".
        MenuBarExtra {
            MenuView(bridge: bridge, settings: bridge.settings)
        } label: {
            Image(systemName: bridge.statusIconName)
        }
        .menuBarExtraStyle(.menu)

        // Standard macOS "Settings" scene — opens via Cmd+, and via our menu.
        Settings {
            SettingsView(bridge: bridge, settings: bridge.settings)
                .frame(width: 460)
        }
    }
}
