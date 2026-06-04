import Foundation
import Network
import OSLog

private let log = Logger(subsystem: "as.trv.qlab-cue-viewer", category: "qlab")

// QLab 5 OSC-over-TCP client (SLIP-framed).
//
// We started on OSC/UDP and hit macOS's net.inet.udp.maxdgram (9216 bytes by
// default) — the kernel silently truncates anything larger, and QLab's reply
// to /runningOrPausedCues for a moderately deep show easily blows past 13KB.
// TCP is a stream with no per-message cap; QLab frames each OSC message with
// SLIP (RFC 1055) so we can find message boundaries in the byte stream.
//
// SLIP framing:
//   • Each OSC packet is wrapped in 0xC0 (END) bytes at start and end.
//   • Literal 0xC0 inside the payload is escaped as 0xDB 0xDC.
//   • Literal 0xDB is escaped as 0xDB 0xDD.
// (QLab tolerates a missing leading END; we always emit it for symmetry.)
//
// On the wire OSC is identical to the UDP transport — same address+typeTag+
// args structure, same {workspace_id, address, status, data} JSON payload in
// the single string arg of /reply messages.

actor QLabClient {
    // Per-cue payload — what the viewer needs to draw one timeline lane.
    struct CueInfo: Codable, Equatable {
        let id: String
        let name: String
        let number: String?
        let type: String              // "Video", "Audio", "Memo", "Group", …
        let groupPath: [String]       // ancestor group names from outermost to immediate parent
        let preWait: Double?
        let preWaitElapsed: Double?
        let duration: Double?
        let elapsed: Double?
        let percent: Double?
    }

    struct Snapshot: Codable, Equatable {
        let ts: Double
        let activeName: String?
        let playheadName: String?
        let playheadNumber: String?
        let groupPath: [String]
        let running: [CueInfo]

        // Exclude `ts` from equality so identical state across polls dedupes
        // and the menu doesn't redraw 4× per second.
        static func == (lhs: Snapshot, rhs: Snapshot) -> Bool {
            lhs.activeName == rhs.activeName
                && lhs.playheadName == rhs.playheadName
                && lhs.playheadNumber == rhs.playheadNumber
                && lhs.groupPath == rhs.groupPath
                && lhs.running == rhs.running
        }
    }

    private let host: String
    private let port: UInt16
    private let passcode: String
    private let pollInterval: TimeInterval
    private let onSnapshot: @Sendable (Snapshot) -> Void
    private let onState: @Sendable (Bool) -> Void

    private var conn: NWConnection?
    private var pollTask: Task<Void, Never>?
    private var isRunning = false

    // SLIP receive buffer — accumulates stream bytes between END markers.
    private var rxBuffer = Data()

    // address (workspace-stripped) → most recent `data` value from QLab
    private var latest: [String: Any] = [:]

    init(host: String,
         port: UInt16,
         passcode: String,
         pollInterval: TimeInterval,
         onSnapshot: @escaping @Sendable (Snapshot) -> Void,
         onState: @escaping @Sendable (Bool) -> Void) {
        self.host = host
        self.port = port
        self.passcode = passcode
        self.pollInterval = pollInterval
        self.onSnapshot = onSnapshot
        self.onState = onState
    }

    // ─── lifecycle ───────────────────────────────────────────────────────────

    func start() {
        isRunning = true
        Task { await connect() }
    }

    func stop() {
        isRunning = false
        pollTask?.cancel(); pollTask = nil
        conn?.cancel(); conn = nil
        rxBuffer.removeAll(keepingCapacity: false)
        onState(false)
    }

    private func connect() async {
        guard isRunning else { return }
        guard let nwPort = NWEndpoint.Port(rawValue: port) else {
            onState(false); return
        }

        let conn = NWConnection(host: NWEndpoint.Host(host), port: nwPort, using: .tcp)
        self.conn = conn

        let readyGate = OnceGate<Bool>()
        conn.stateUpdateHandler = { [weak self] state in
            switch state {
            case .ready:
                readyGate.fire(true)
            case .failed(let err):
                log.error("TCP failed: \(err.localizedDescription, privacy: .public)")
                readyGate.fire(false)
                Task { await self?.handleDisconnect() }
            case .cancelled:
                readyGate.fire(false)
            default:
                break
            }
        }
        conn.start(queue: .global(qos: .utility))

        guard await readyGate.wait(), isRunning else {
            conn.cancel()
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }
        log.info("TCP connected to \(self.host, privacy: .public):\(self.port)")
        onState(true)

        // Continuous read loop — pulls stream chunks and feeds the SLIP
        // de-framer. Starts as soon as the connection is ready; QLab won't
        // send anything until we send something first, but priming the loop
        // means we never miss the first reply.
        scheduleRead()

        // /alwaysReply first so /connect's response on passcode-protected
        // workspaces comes back.
        send(OSCMessage("/alwaysReply", args: [.int32(1)]))
        if !passcode.isEmpty {
            send(OSCMessage("/connect", args: [.string(passcode)]))
        }
        startPolling()
    }

    private func handleDisconnect() async {
        onState(false)
        conn = nil
        rxBuffer.removeAll(keepingCapacity: false)
        if isRunning { await scheduleReconnect() }
    }

    private func scheduleReconnect() async {
        try? await Task.sleep(nanoseconds: 2 * 1_000_000_000)
        await connect()
    }

    // ─── send + receive ──────────────────────────────────────────────────────

    private func send(_ msg: OSCMessage) {
        guard let conn else { return }
        let framed = SLIP.encode(msg.encoded)
        conn.send(content: framed, completion: .contentProcessed { err in
            if let err {
                log.error("TCP send failed: \(err.localizedDescription, privacy: .public)")
            }
        })
    }

    private nonisolated func scheduleRead() {
        Task { await self._scheduleRead() }
    }

    private func _scheduleRead() {
        guard let conn else { return }
        conn.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, err in
            if let data, !data.isEmpty {
                Task { await self?.feed(data) }
            }
            if let err {
                log.error("TCP receive: \(err.localizedDescription, privacy: .public)")
                Task { await self?.handleDisconnect() }
                return
            }
            if isComplete {
                Task { await self?.handleDisconnect() }
                return
            }
            // Re-arm
            Task { await self?._scheduleRead() }
        }
    }

    private func feed(_ chunk: Data) {
        rxBuffer.append(chunk)
        // Split on END bytes — each complete segment is one SLIP-encoded
        // OSC message. Multiple messages can arrive in a single TCP read,
        // and a single message can be split across reads.
        while let endIdx = rxBuffer.firstIndex(of: SLIP.END) {
            let segment = rxBuffer.subdata(in: rxBuffer.startIndex..<endIdx)
            rxBuffer.removeSubrange(rxBuffer.startIndex...endIdx)
            // Skip empty segments (the leading END byte of the next packet).
            guard !segment.isEmpty else { continue }
            let decoded = SLIP.decode(segment)
            handleReceived(decoded)
        }
    }

    private func handleReceived(_ data: Data) {
        guard let msg = OSCMessage.decode(data) else { return }

        // QLab reply format: address starts with "/reply", single string arg
        // containing a JSON object: { address, status, data, workspace_id }.
        // The "address" inside is the workspace-prefixed canonical form, e.g.
        // /workspace/<id>/cue/playhead/displayName. We strip the workspace
        // prefix back off so the key matches what we asked for.
        if msg.address.hasPrefix("/reply"),
           case .string(let json)? = msg.args.first,
           let obj = try? JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any] {
            var askedFor = String(msg.address.dropFirst("/reply".count))
            if askedFor.isEmpty, let inner = obj["address"] as? String {
                askedFor = stripWorkspacePrefix(inner)
            }
            if let value = obj["data"] {
                latest[askedFor] = value
            }
        }
    }

    private func stripWorkspacePrefix(_ s: String) -> String {
        if s.hasPrefix("/workspace/") {
            let afterPrefix = s.dropFirst("/workspace/".count)
            if let slash = afterPrefix.firstIndex(of: "/") {
                return String(afterPrefix[slash...])
            }
        }
        return s
    }

    // ─── polling ─────────────────────────────────────────────────────────────

    private func startPolling() {
        pollTask?.cancel()
        let intervalNs = UInt64(pollInterval * 1_000_000_000)
        pollTask = Task { [weak self] in
            while !Task.isCancelled, await self?.isRunning == true {
                await self?.pollOnce()
                try? await Task.sleep(nanoseconds: intervalNs)
            }
        }
    }

    private func pollOnce() async {
        // First-tier queries: things that don't depend on knowing cue IDs.
        send(OSCMessage("/cue/playhead/displayName"))
        send(OSCMessage("/cue/playhead/number"))
        send(OSCMessage("/cue/active/displayName"))
        send(OSCMessage("/runningOrPausedCues"))

        let (cues, groupPath) = flattenRunning(latest["/runningOrPausedCues"])

        // Cache hygiene: evict any cached /cue_id/<uuid>/... entries whose
        // cue is no longer in the running list. Without this, a cue that
        // briefly leaves and comes back (or a different cue at the same
        // uniqueID after a workspace reload) would publish using stale
        // values from a previous run, briefly placing the bar in the wrong
        // spot or mis-classifying duration.
        let runningIds = Set(cues.map { $0.id })
        for key in Array(latest.keys) where key.hasPrefix("/cue_id/") {
            // Key shape: "/cue_id/<UUID>/<property>". Pull out <UUID>.
            let after = key.dropFirst("/cue_id/".count)
            if let slash = after.firstIndex(of: "/") {
                let id = String(after[..<slash])
                if !runningIds.contains(id) {
                    latest.removeValue(forKey: key)
                }
            }
        }

        // Second-tier: per-cue duration / elapsed / progress for everything
        // we know is running from the LAST tick.
        for cue in cues {
            send(OSCMessage("/cue_id/\(cue.id)/preWait"))
            send(OSCMessage("/cue_id/\(cue.id)/preWaitElapsed"))
            send(OSCMessage("/cue_id/\(cue.id)/duration"))
            send(OSCMessage("/cue_id/\(cue.id)/actionElapsed"))
            send(OSCMessage("/cue_id/\(cue.id)/percentActionElapsed"))
        }

        // First-sight gate: drop any cue whose static-ish properties
        // (preWait OR duration) haven't replied yet. The first poll tick
        // after a cue appears would otherwise publish it with nil-everywhere
        // and the viewer would briefly treat it as an instant cue anchored
        // at the song's start — exactly the "phantom flash" we hit when
        // clicking through cues fast. One ~250ms poll cycle later, the
        // properties are back and the cue lands properly.
        let running: [CueInfo] = cues.compactMap { stub in
            let pw  = numberValue(latest["/cue_id/\(stub.id)/preWait"])
            let dur = numberValue(latest["/cue_id/\(stub.id)/duration"])
            guard pw != nil || dur != nil else { return nil }
            return CueInfo(
                id:             stub.id,
                name:           stub.name,
                number:         stub.number,
                type:           stub.type,
                groupPath:      stub.groupPath,
                preWait:        pw,
                preWaitElapsed: numberValue(latest["/cue_id/\(stub.id)/preWaitElapsed"]),
                duration:       dur,
                elapsed:        numberValue(latest["/cue_id/\(stub.id)/actionElapsed"]),
                percent:        numberValue(latest["/cue_id/\(stub.id)/percentActionElapsed"])
            )
        }

        let snap = Snapshot(
            ts: Date().timeIntervalSince1970 * 1000,
            activeName:     latest["/cue/active/displayName"]   as? String,
            playheadName:   latest["/cue/playhead/displayName"] as? String,
            playheadNumber: latest["/cue/playhead/number"]      as? String,
            groupPath:      groupPath,
            running:        running
        )
        onSnapshot(snap)
    }

    // ─── /runningOrPausedCues tree walking — same as UDP version ─────────────

    private struct RunningStub {
        let id, name, type: String
        let number: String?
        let groupPath: [String]
    }

    private func flattenRunning(_ raw: Any?) -> ([RunningStub], [String]) {
        guard let arr = raw as? [[String: Any]] else { return ([], []) }
        var occurrences: [RunningStub] = []
        var deepestPath: [String] = []
        for item in arr {
            let walked = collect(into: &occurrences, path: [], item)
            if walked.count > deepestPath.count { deepestPath = walked }
        }
        var bestForId: [String: RunningStub] = [:]
        var orderedIds: [String] = []
        for stub in occurrences {
            if let existing = bestForId[stub.id] {
                if stub.groupPath.count > existing.groupPath.count {
                    bestForId[stub.id] = stub
                }
            } else {
                orderedIds.append(stub.id)
                bestForId[stub.id] = stub
            }
        }
        return (orderedIds.compactMap { bestForId[$0] }, deepestPath)
    }

    @discardableResult
    private func collect(into out: inout [RunningStub],
                         path: [String],
                         _ item: [String: Any]) -> [String] {
        let children = item["cues"] as? [[String: Any]] ?? []
        let displayName = (item["listName"] as? String).flatMap { $0.isEmpty ? nil : $0 }
            ?? (item["name"] as? String).flatMap { $0.isEmpty ? nil : $0 }

        if !children.isEmpty {
            let newPath = displayName.map { path + [$0] } ?? path
            var deepest = newPath
            for child in children {
                let walked = collect(into: &out, path: newPath, child)
                if walked.count > deepest.count { deepest = walked }
            }
            return deepest
        }
        guard let id = item["uniqueID"] as? String else { return path }
        let leafName = displayName ?? "Unnamed"
        let number = (item["number"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        let type = (item["type"] as? String) ?? "Cue"
        out.append(RunningStub(id: id, name: leafName, type: type, number: number, groupPath: path))
        return path
    }

    private func numberValue(_ raw: Any?) -> Double? {
        if let d = raw as? Double { return d }
        if let i = raw as? Int { return Double(i) }
        if let s = raw as? String { return Double(s) }
        return nil
    }
}

// MARK: - SLIP framing (RFC 1055)

private enum SLIP {
    static let END: UInt8 = 0xC0
    static let ESC: UInt8 = 0xDB
    static let ESC_END: UInt8 = 0xDC
    static let ESC_ESC: UInt8 = 0xDD

    /// Wrap `payload` with leading + trailing END markers and escape any
    /// literal END/ESC bytes inside it.
    static func encode(_ payload: Data) -> Data {
        var out = Data()
        out.reserveCapacity(payload.count + 4)
        out.append(END)
        for byte in payload {
            switch byte {
            case END: out.append(ESC); out.append(ESC_END)
            case ESC: out.append(ESC); out.append(ESC_ESC)
            default:  out.append(byte)
            }
        }
        out.append(END)
        return out
    }

    /// Un-escape the inside of a SLIP segment (the bytes BETWEEN END markers).
    /// We pass these in already-split, so this routine doesn't look for END.
    static func decode(_ segment: Data) -> Data {
        var out = Data()
        out.reserveCapacity(segment.count)
        var i = segment.startIndex
        while i < segment.endIndex {
            let b = segment[i]
            if b == ESC, segment.index(after: i) < segment.endIndex {
                let next = segment[segment.index(after: i)]
                switch next {
                case ESC_END: out.append(END)
                case ESC_ESC: out.append(ESC)
                default:      out.append(next)   // tolerant: pass through
                }
                i = segment.index(i, offsetBy: 2)
            } else {
                out.append(b)
                i = segment.index(after: i)
            }
        }
        return out
    }
}

// MARK: - OnceGate
//
// One-shot continuation wrapper. Lets a non-isolated callback (like
// NWConnection.stateUpdateHandler, called on a Network.framework queue) resume
// an `await` exactly once without tripping Swift 6's strict concurrency rules.

private final class OnceGate<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var cont: CheckedContinuation<T, Never>?
    private var pending: T?

    func attach(_ continuation: CheckedContinuation<T, Never>) {
        lock.lock(); defer { lock.unlock() }
        if let pending {
            continuation.resume(returning: pending)
            self.pending = nil
        } else {
            self.cont = continuation
        }
    }

    func fire(_ value: T) {
        lock.lock(); defer { lock.unlock() }
        if let cont {
            cont.resume(returning: value)
            self.cont = nil
        } else if pending == nil {
            pending = value
        }
    }

    func wait() async -> T {
        await withCheckedContinuation { (cont: CheckedContinuation<T, Never>) in
            attach(cont)
        }
    }
}

// MARK: - OSC binary encoding (the minimum we need)
//
// Identical to the UDP version: emit /address with optional int32/string args;
// parse QLab replies (single string arg containing JSON).

struct OSCMessage {
    enum Arg { case int32(Int32); case string(String) }

    let address: String
    let args: [Arg]

    init(_ address: String, args: [Arg] = []) {
        self.address = address
        self.args = args
    }

    var encoded: Data {
        var data = Data()
        data.append(Self.padString(address))
        var typeTag = ","
        for arg in args {
            switch arg {
            case .int32:  typeTag.append("i")
            case .string: typeTag.append("s")
            }
        }
        data.append(Self.padString(typeTag))
        for arg in args {
            switch arg {
            case .int32(let v):
                var be = v.bigEndian
                data.append(Data(bytes: &be, count: 4))
            case .string(let s):
                data.append(Self.padString(s))
            }
        }
        return data
    }

    private static func padString(_ s: String) -> Data {
        var d = Data(s.utf8)
        d.append(0)
        let pad = (4 - d.count % 4) % 4
        if pad > 0 { d.append(contentsOf: [UInt8](repeating: 0, count: pad)) }
        return d
    }

    static func decode(_ data: Data) -> OSCMessage? {
        var cursor = data.startIndex
        guard let address = readPaddedString(data, &cursor) else { return nil }
        guard let typeTag = readPaddedString(data, &cursor), typeTag.hasPrefix(",") else {
            return OSCMessage(address)
        }
        var args: [Arg] = []
        for ch in typeTag.dropFirst() {
            switch ch {
            case "i":
                guard cursor + 4 <= data.endIndex else { return nil }
                let v = data[cursor..<cursor+4].withUnsafeBytes { $0.load(as: Int32.self).bigEndian }
                args.append(.int32(v))
                cursor += 4
            case "s":
                guard let s = readPaddedString(data, &cursor) else { return nil }
                args.append(.string(s))
            default:
                return OSCMessage(address, args: args)
            }
        }
        return OSCMessage(address, args: args)
    }

    private static func readPaddedString(_ data: Data, _ cursor: inout Data.Index) -> String? {
        guard cursor < data.endIndex else { return nil }
        var end = cursor
        while end < data.endIndex, data[end] != 0 { end += 1 }
        guard end < data.endIndex else { return nil }
        let s = String(data: data[cursor..<end], encoding: .utf8) ?? ""
        let strLen = end - cursor + 1
        let padded = strLen + (4 - strLen % 4) % 4
        cursor += padded
        return s
    }
}
