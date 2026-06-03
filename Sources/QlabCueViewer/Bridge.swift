import Foundation
import AppKit
import SwiftUI

// Coordinator. Owns the settings, the QLab client, and the relay publisher;
// reconnects them when settings change; exposes published state for the UI.
@MainActor
final class Bridge: ObservableObject {
    enum LinkState { case off, connecting, on }

    @Published var qlabState: LinkState = .off
    @Published var relayState: LinkState = .off
    @Published var lastSnapshot: QLabClient.Snapshot?
    @Published var lastPublishedAt: Date?
    @Published var lastError: String?

    // Owned, not @ObservedObject — views that care take settings as a separate
    // @ObservedObject parameter so changes there don't re-render Bridge clients.
    let settings = SettingsStore()

    private var qlab: QLabClient?
    private var relay: RelayPublisher?

    init() {
        // App-level setup: NSApp policy stays default; MenuBarExtra works in
        // either accessory or regular mode but we want no dock icon.
        NSApp?.setActivationPolicy(.accessory)
        if settings.autoConnect { start() }
    }

    // ─── public API for the menu / settings view ─────────────────────────────

    func start() {
        stop()  // ensure clean slate so settings changes always apply
        guard let relayURL = URL(string: settings.relayURL) else {
            lastError = "Invalid relay URL in settings"
            return
        }
        let trimmedHost = settings.qlabHost.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedHost.isEmpty,
              let qlabPort = UInt16(exactly: settings.qlabPort), qlabPort > 0 else {
            lastError = "Invalid QLab host/port"
            return
        }

        qlabState = .connecting
        relayState = .connecting

        // Trim whitespace/newlines from pasted secrets — a `pbcopy < file` on
        // a token saved with `echo` ships an invisible trailing `\n` that the
        // server rejects, with no obvious user-facing clue why.
        let trimmedToken   = settings.relayToken.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedChannel = settings.relayChannel.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedPass    = settings.qlabPasscode.trimmingCharacters(in: .whitespacesAndNewlines)

        let relay = RelayPublisher(
            baseURL: relayURL,
            token: trimmedToken,
            channel: trimmedChannel,
            onState: { [weak self] up in
                Task { @MainActor in self?.relayState = up ? .on : .off }
            }
        )
        self.relay = relay

        let qlab = QLabClient(
            host: trimmedHost,
            port: qlabPort,
            passcode: trimmedPass,
            pollInterval: TimeInterval(settings.pollIntervalMs) / 1000,
            onSnapshot: { [weak self] snap in
                Task { @MainActor in
                    // Dedupe: only push to @Published (and across the wire)
                    // when the snapshot actually differs. Stops the menu from
                    // resetting selection on every idle poll.
                    guard self?.lastSnapshot != snap else { return }
                    self?.lastSnapshot = snap
                    self?.lastPublishedAt = Date()
                    Task { await relay.publish(snap) }
                }
            },
            onState: { [weak self] up in
                Task { @MainActor in self?.qlabState = up ? .on : .off }
            }
        )
        self.qlab = qlab

        Task { await relay.start() }
        Task { await qlab.start() }
    }

    func stop() {
        if let relay = relay { Task { await relay.stop() } }
        if let qlab = qlab   { Task { await qlab.stop() } }
        qlab = nil; relay = nil
        qlabState = .off; relayState = .off
    }

    func restart() { start() }

    var isRunning: Bool { qlab != nil }

    // ─── menu bar icon ───────────────────────────────────────────────────────

    var statusIconName: String {
        switch (qlabState, relayState) {
        case (.on, .on):                                 return "dot.radiowaves.left.and.right"
        case (.on, _), (_, .on), (.connecting, _), (_, .connecting): return "antenna.radiowaves.left.and.right.slash"
        default:                                         return "antenna.radiowaves.left.and.right.slash"
        }
    }
}
