import Foundation
import Network
import OSLog

private let log = Logger(subsystem: "as.trv.qlab-cue-viewer", category: "ltc")

// What the viewer gets. `running` is the difference between "the show is
// playing and this is where it is" and "playback stopped, this is where it
// stopped" — the viewer freezes the readout on the latter rather than letting
// it free-run off the end.
struct TimecodeInfo: Codable, Equatable {
    let text: String        // "09:25:12:07"
    let fps: Double         // 25, 29.97, …
    let dropFrame: Bool
    let running: Bool
    let source: String      // "ltc"
    let label: String?      // whatever was typed into the plug-in, if anything
}

// Receives `/ltc/frame` from the LTC Tap audio plug-in over OSC/UDP.
//
// The plug-in sits on the QLab cue output carrying LTC and decodes the actual
// timecode in the audio, so this is the real thing rather than a position
// derived from QLab's own elapsed counters.
//
// Bound to loopback only. Nothing outside this Mac can reach it, and nothing
// outside this Mac should be able to inject a timecode into the show's viewer.
//
// Default port is 57121, not the more obvious 53535 — QLab already binds that
// one (along with 53000, 53354, 54000 and 59599), so a listener there fails
// with EADDRINUSE on exactly the machine this is meant to run on.
final class LTCListener: @unchecked Sendable {
    private struct Sender {
        var text: String
        var fpsX100: Int
        var dropFrame: Bool
        var locked: Bool
        var label: String
        var seq: Int32
        var at: Date
        var lastLockedAt: Date?     // when this sender was last actually moving
        var lockedSince: Date?      // when it STARTED moving; nil while stopped
    }

    // A frame is "current" for a little longer than one frame at any sane rate,
    // so a late packet doesn't blink the readout off.
    private static let liveWindow: TimeInterval  = 0.4
    // How long a stopped sender's last value stays on screen before we decide
    // the plug-in has gone away entirely.
    private static let frozenWindow: TimeInterval = 10.0

    private let lock = NSLock()
    private var senders: [String: Sender] = [:]
    private var listener: NWListener?
    private var connections: [NWConnection] = []

    // ─── lifecycle ───────────────────────────────────────────────────────────

    func start(port: UInt16) {
        stop()
        guard let nwPort = NWEndpoint.Port(rawValue: port) else { return }

        let params = NWParameters.udp
        params.allowLocalEndpointReuse = true
        params.requiredLocalEndpoint = .hostPort(host: .ipv4(.loopback), port: nwPort)

        do {
            let l = try NWListener(using: params)
            l.newConnectionHandler = { [weak self] conn in
                self?.accept(conn)
            }
            l.stateUpdateHandler = { state in
                if case .failed(let err) = state {
                    log.error("LTC listener failed: \(err.localizedDescription, privacy: .public)")
                }
            }
            l.start(queue: .global(qos: .utility))
            listener = l
            log.info("LTC listener on 127.0.0.1:\(port)")
        } catch {
            log.error("LTC listener could not bind \(port): \(error.localizedDescription, privacy: .public)")
        }
    }

    func stop() {
        listener?.cancel(); listener = nil
        lock.lock()
        connections.forEach { $0.cancel() }
        connections.removeAll()
        senders.removeAll()
        lock.unlock()
    }

    private func accept(_ conn: NWConnection) {
        lock.lock(); connections.append(conn); lock.unlock()
        conn.start(queue: .global(qos: .utility))
        receive(on: conn)
    }

    private func receive(on conn: NWConnection) {
        conn.receiveMessage { [weak self] data, _, _, err in
            guard let self else { return }
            if let data, !data.isEmpty { self.handle(data) }
            if err != nil {
                conn.cancel()
                self.lock.lock()
                self.connections.removeAll { $0 === conn }
                self.lock.unlock()
                return
            }
            self.receive(on: conn)
        }
    }

    // ─── parsing ─────────────────────────────────────────────────────────────

    // /ltc/frame  s:instanceId  s:label  s:"HH:MM:SS:FF"  i:fpsX100  i:seq  i:flags
    private func handle(_ data: Data) {
        guard let msg = OSCMessage.decode(data), msg.address == "/ltc/frame",
              msg.args.count >= 6,
              case .string(let id)    = msg.args[0],
              case .string(let label) = msg.args[1],
              case .string(let text)  = msg.args[2],
              case .int32(let fps)    = msg.args[3],
              case .int32(let seq)    = msg.args[4],
              case .int32(let flags)  = msg.args[5]
        else { return }

        lock.lock(); defer { lock.unlock() }

        // UDP can reorder, and a plug-in instance that gets rebuilt starts its
        // counter again — so only reject a packet that is behind one we
        // already have from a sender we're still hearing from.
        if let existing = senders[id], seq < existing.seq,
           Date().timeIntervalSince(existing.at) < Self.liveWindow {
            return
        }

        let now = Date()
        let locked = flags & 1 != 0
        let previous = senders[id]

        // Stamp when this sender STARTED running, and leave it alone for as
        // long as it keeps running. That timestamp is what makes the choice
        // below stable — see current().
        let lockedSince: Date?
        if !locked                          { lockedSince = nil }
        else if previous?.locked == true    { lockedSince = previous?.lockedSince ?? now }
        else                                { lockedSince = now }

        senders[id] = Sender(text: text,
                             fpsX100: Int(fps),
                             dropFrame: flags & 2 != 0,
                             locked: locked,
                             label: label,
                             seq: seq,
                             at: now,
                             lastLockedAt: locked ? now : previous?.lastLockedAt,
                             lockedSince: lockedSince)
    }

    // ─── what the bridge publishes ───────────────────────────────────────────

    func current() -> TimecodeInfo? {
        lock.lock(); defer { lock.unlock() }

        let now = Date()
        senders = senders.filter { now.timeIntervalSince($0.value.at) < Self.frozenWindow }

        // Several cues can be running at once, so several plug-in instances can
        // be sending — and at a song change the outgoing cue is usually still
        // sounding while the next one starts.
        //
        // Pick the one that started running most recently, NOT the one heard
        // from most recently. Two locked senders on independent 30 Hz timers
        // interleave their packets, so "freshest packet" alternates between
        // them several times a second and the readout flips between two songs'
        // timecodes — which is exactly what a jumping hour digit looks like.
        // `lockedSince` doesn't change while a sender keeps running, so this
        // switches once, cleanly, to the cue that just started.
        let live = senders.values.filter { $0.locked && now.timeIntervalSince($0.at) < Self.liveWindow }

        // When nothing is moving, freeze on the one that ran most recently —
        // NOT simply the one heard from most recently. Instances on loaded but
        // idle cues keep heartbeating, and picking those would replace the
        // frame the show actually stopped on with an unrelated cue's origin.
        let lastRan = senders.values
            .filter { $0.lastLockedAt != nil }
            .max { ($0.lastLockedAt ?? .distantPast) < ($1.lastLockedAt ?? .distantPast) }

        let newest = live.max { ($0.lockedSince ?? .distantPast) < ($1.lockedSince ?? .distantPast) }

        guard let best = newest
                      ?? lastRan
                      ?? senders.values.max(by: { $0.at < $1.at })
        else { return nil }

        return TimecodeInfo(
            text:      best.text,
            fps:       Double(best.fpsX100) / 100.0,
            dropFrame: best.dropFrame,
            running:   best.locked && now.timeIntervalSince(best.at) < Self.liveWindow,
            source:    "ltc",
            label:     best.label.isEmpty ? nil : best.label
        )
    }
}
