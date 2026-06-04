import Foundation
import Network
import OSLog

private let log = Logger(subsystem: "as.trv.qlab-cue-viewer", category: "qlab")

// QLab 5 OSC-over-UDP client.
//
// Protocol summary (what we actually use):
//   • OSC binary packet = padded-string address, padded-string type tag
//     (e.g. ",s"), then args.
//   • With `/alwaysReply 1`, QLab replies to every command with an OSC message
//     whose address is "/reply" + original address, carrying a single JSON
//     string arg: `{"workspace_id":"…","address":"…","status":"ok","data":…}`.
//   • Replies come back to the source port the request was sent from. NWConnection
//     in UDP mode handles that automatically.
//
// v0 just polls a handful of /cue/playhead/… and /runningOrPausedCues addresses
// at the configured interval and stitches the latest replies into a Snapshot.
// We'll switch to push-style updates once API discovery tells us which ones
// QLab will actually emit unprompted.

actor QLabClient {
    // Per-cue payload — what the viewer needs to draw one timeline lane.
    struct CueInfo: Codable, Equatable {
        let id: String
        let name: String
        let number: String?
        let type: String              // "Video", "Audio", "Memo", "Group", …
        let groupPath: [String]       // ancestor group names from outermost to immediate parent
        let preWait: Double?          // seconds offset from group start before the cue fires
        let preWaitElapsed: Double?   // how much of the preWait has elapsed (= countdown bar fill)
        let duration: Double?         // seconds — total run length of the cue's action
        let elapsed: Double?          // seconds since the cue's action started (post preWait)
        let percent: Double?          // 0..1 progress (canonical for the bar fill)
    }

    struct Snapshot: Codable, Equatable {
        let ts: Double
        let activeName: String?
        let playheadName: String?
        let playheadNumber: String?
        // Names of the running group ancestors of the leaf cues, outermost
        // first — e.g. ["SHOW 1", "SONG"]. Lets the viewer show breadcrumb
        // context above the playhead name.
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

    // Two-socket setup because NWConnection in "connected UDP" mode filters
    // incoming datagrams by source endpoint — and QLab replies from a port
    // distinct from the request port. Sending via a connected NWConnection,
    // receiving via an NWListener that accepts from any source, is the model
    // that works with QLab 5.
    private var sender: NWConnection?
    private var listener: NWListener?
    private var localPort: UInt16 = 0
    private var pollTask: Task<Void, Never>?
    private var isRunning = false

    // address → most recent `data` value from QLab (any JSON type)
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
        sender?.cancel(); sender = nil
        listener?.cancel(); listener = nil
        onState(false)
    }

    private func connect() async {
        guard isRunning else { return }
        guard let nwPort = NWEndpoint.Port(rawValue: port) else {
            onState(false); return
        }

        // ── Listener: accept incoming UDP from any source on an OS-picked
        //    local port. This is what QLab will reply to.
        let listener: NWListener
        do {
            listener = try NWListener(using: .udp)
        } catch {
            log.error("NWListener creation failed: \(error.localizedDescription, privacy: .public)")
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }
        self.listener = listener

        let listenerReadyGate = OnceGate<Bool>()
        listener.stateUpdateHandler = { state in
            switch state {
            case .ready:              listenerReadyGate.fire(true)
            case .failed, .cancelled: listenerReadyGate.fire(false)
            default:                  break
            }
        }
        listener.newConnectionHandler = { [weak self] newConn in
            // QLab's source address shows up here. Each unique remote yields
            // one connection. Start it and hand it to the actor for reads.
            Task { await self?.acceptIncoming(newConn) }
        }
        listener.start(queue: .global(qos: .utility))

        guard await listenerReadyGate.wait(), isRunning else {
            listener.cancel()
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }
        localPort = listener.port?.rawValue ?? 0
        guard localPort != 0 else {
            log.error("Listener has no port after .ready — bailing")
            listener.cancel()
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }
        log.info("Listening for QLab replies on UDP \(self.localPort)")

        // ── Sender: connected NWConnection to QLab's request port.
        let sender = NWConnection(host: NWEndpoint.Host(host), port: nwPort, using: .udp)
        self.sender = sender
        let senderReadyGate = OnceGate<Bool>()
        sender.stateUpdateHandler = { state in
            switch state {
            case .ready:              senderReadyGate.fire(true)
            case .failed, .cancelled: senderReadyGate.fire(false)
            default:                  break
            }
        }
        sender.start(queue: .global(qos: .utility))
        guard await senderReadyGate.wait(), isRunning else {
            sender.cancel()
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }
        log.info("Sender ready to \(self.host, privacy: .public):\(self.port)")
        onState(true)

        announceReplyPort()
        // alwaysReply first so /connect's response (with passcode-protected
        // workspaces) actually comes back.
        send(OSCMessage("/alwaysReply", args: [.int32(1)]))
        if !passcode.isEmpty {
            send(OSCMessage("/connect", args: [.string(passcode)]))
        }

        startPolling()
    }

    // Handle a freshly-arriving remote (QLab). QLab uses a fresh ephemeral
    // source port for every reply, so each "flow" delivers exactly one packet.
    // Read it, process it, cancel the connection — otherwise we'd accumulate
    // hundreds of dead NWConnection objects.
    private func acceptIncoming(_ conn: NWConnection) {
        conn.start(queue: .global(qos: .utility))
        conn.receiveMessage { [weak self] content, _, _, _ in
            if let data = content {
                Task { await self?.handleReceived(data) }
            }
            conn.cancel()
        }
    }

    private func scheduleReconnect() async {
        try? await Task.sleep(nanoseconds: 2 * 1_000_000_000)
        await connect()
    }

    // ─── send + receive ──────────────────────────────────────────────────────

    private func send(_ msg: OSCMessage) {
        guard let sender else { return }
        sender.send(content: msg.encoded, completion: .contentProcessed { _ in })
    }

    private func announceReplyPort() {
        guard localPort != 0 else { return }
        send(OSCMessage("/udpReplyPort", args: [.int32(Int32(localPort))]))
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

    // QLab reply addresses look like /workspace/<UUID>/cue/playhead/displayName,
    // but we asked with /cue/playhead/displayName. Strip the workspace prefix
    // so the cache key matches.
    private func stripWorkspacePrefix(_ s: String) -> String {
        if s.hasPrefix("/workspace/") {
            // /workspace/<uuid>/...rest...
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
        // Re-stake our claim to QLab's reply port every cycle. Cheap (one
        // tiny UDP packet) and immune to other OSC clients overriding it.
        announceReplyPort()

        // First-tier queries: things that don't depend on knowing cue IDs.
        send(OSCMessage("/cue/playhead/displayName"))
        send(OSCMessage("/cue/playhead/number"))
        send(OSCMessage("/cue/active/displayName"))
        send(OSCMessage("/runningOrPausedCues"))

        // Second-tier: per-cue duration / elapsed / progress for everything we
        // know is running from the LAST tick. Means there's a one-poll lag
        // before a fresh cue gets its progress data, which is fine for a 4 Hz
        // refresh rate. Each cue costs 3 tiny UDP packets.
        let (cues, groupPath) = flattenRunning(latest["/runningOrPausedCues"])
        for cue in cues {
            send(OSCMessage("/cue_id/\(cue.id)/preWait"))
            send(OSCMessage("/cue_id/\(cue.id)/preWaitElapsed"))
            send(OSCMessage("/cue_id/\(cue.id)/duration"))
            send(OSCMessage("/cue_id/\(cue.id)/actionElapsed"))
            send(OSCMessage("/cue_id/\(cue.id)/percentActionElapsed"))
        }

        // Build snapshot from cache. Cues that haven't had their per-cue data
        // come back yet appear with nil duration/elapsed/percent.
        let running: [CueInfo] = cues.map { stub in
            CueInfo(
                id:             stub.id,
                name:           stub.name,
                number:         stub.number,
                type:           stub.type,
                groupPath:      stub.groupPath,
                preWait:        numberValue(latest["/cue_id/\(stub.id)/preWait"]),
                preWaitElapsed: numberValue(latest["/cue_id/\(stub.id)/preWaitElapsed"]),
                duration:       numberValue(latest["/cue_id/\(stub.id)/duration"]),
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

    // Walk QLab's nested /runningOrPausedCues tree, return one flat entry per
    // *leaf* cue (skipping Group / Cue List containers). The user wants lanes
    // for things that actually produce output (Video, Audio, Memo, …), not
    // for the groups holding them.
    private struct RunningStub {
        let id, name, type: String
        let number: String?
        let groupPath: [String]   // its parent chain — used to filter to the current song
    }

    private func flattenRunning(_ raw: Any?) -> ([RunningStub], [String]) {
        guard let arr = raw as? [[String: Any]] else { return ([], []) }
        // /runningOrPausedCues returns each running cue at the top level AND
        // again nested inside its parent group, so a naive walk yields each
        // leaf twice. Dedup by uniqueID, keeping the first occurrence (which
        // preserves the source order QLab gave us). At the same time, track
        // the deepest group-name chain we walk through so the viewer can show
        // a "SHOW 1 › SONG" style breadcrumb.
        var out: [RunningStub] = []
        var seen = Set<String>()
        var deepestPath: [String] = []
        for item in arr {
            let walked = collect(into: &out, seen: &seen, path: [], item)
            if walked.count > deepestPath.count { deepestPath = walked }
        }
        return (out, deepestPath)
    }

    @discardableResult
    private func collect(into out: inout [RunningStub],
                         seen: inout Set<String>,
                         path: [String],
                         _ item: [String: Any]) -> [String] {
        let children = item["cues"] as? [[String: Any]] ?? []
        let displayName = (item["listName"] as? String).flatMap { $0.isEmpty ? nil : $0 }
            ?? (item["name"] as? String).flatMap { $0.isEmpty ? nil : $0 }

        if !children.isEmpty {
            // It's a group — recurse with its name appended to the path.
            let newPath = displayName.map { path + [$0] } ?? path
            var deepest = newPath
            for child in children {
                let walked = collect(into: &out, seen: &seen, path: newPath, child)
                if walked.count > deepest.count { deepest = walked }
            }
            return deepest
        }
        // Leaf — record it; the path we got is its parent chain.
        guard let id = item["uniqueID"] as? String, !seen.contains(id) else { return path }
        seen.insert(id)
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

    // Sugar for `await withCheckedContinuation { gate.attach($0) }`.
    func wait() async -> T {
        await withCheckedContinuation { (cont: CheckedContinuation<T, Never>) in
            attach(cont)
        }
    }
}

// MARK: - OSC binary encoding (the minimum we need)
//
// We only emit `/address` with optional int32 and string args, and only parse
// QLab replies (single string arg containing JSON). A real OSC library would
// support floats, blobs, timetags, bundles, etc. — we don't need them for the
// query/reply traffic this bridge does.

struct OSCMessage {
    enum Arg { case int32(Int32); case string(String) }

    let address: String
    let args: [Arg]

    init(_ address: String, args: [Arg] = []) {
        self.address = address
        self.args = args
    }

    // Encode → Data. Strings are null-terminated and padded to a 4-byte
    // boundary; ints are big-endian; type tag string starts with ','.
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

    // Decode → Message. We only need address + the first string arg in QLab's
    // reply path, but we parse all args we recognize for completeness.
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
                // Unknown type — bail rather than mis-parse the rest.
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
        let strLen = end - cursor + 1               // include the null
        let padded = strLen + (4 - strLen % 4) % 4  // up to next 4-byte boundary
        cursor += padded
        return s
    }
}
