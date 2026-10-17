import Foundation

// Publishes JSON snapshots to relay-server as a "publisher" on a channel.
// Drops messages while disconnected — the relay caches the last text payload
// per channel, so a freshly-connecting subscriber always sees latest state.
//
// Connection health. A relay restart behind a reverse proxy can leave the
// client side of the socket half-open: `receive()` just waits, and a failed
// `send` used to be swallowed, so the publisher believed it was connected and
// never reconnected (seen when relay-server was redeployed mid-session).
// Three things now catch a dead connection:
//   • a ping every `pingInterval`; no pong within `pongTimeout` → drop
//   • a failed send → drop
//   • `receive()` throwing → drop
// All three go through `drop(_:)`, which is idempotent per socket, and a
// generation counter makes sure only one reconnect loop is ever alive.

actor RelayPublisher {
    static let pingInterval: TimeInterval = 5
    static let pongTimeout: TimeInterval = 5

    private let baseURL: URL
    private let token: String
    private let channel: String
    private let onState: @Sendable (Bool) -> Void

    private var task: URLSessionWebSocketTask?
    private var session: URLSession?
    private var keepalive: Task<Void, Never>?
    private var reconnectDelay: TimeInterval = 1
    private var isRunning = false
    private var connected = false
    // Bumped on start/stop and on every drop. A connect or reconnect loop
    // carrying an older generation gives up, so a stale loop can never race a
    // newer one into opening two sockets.
    private var generation = 0
    // Latest payload buffered while disconnected. Replayed the instant we
    // (re)connect. Solves the start-up race where the first QLab snapshot
    // fires before the WebSocket handshake finishes — without this, the
    // dedup in Bridge skips every subsequent identical snapshot and the
    // viewer never sees anything until QLab state actually changes.
    private var pendingPayload: String?

    init(baseURL: URL,
         token: String,
         channel: String,
         onState: @escaping @Sendable (Bool) -> Void) {
        self.baseURL = baseURL
        self.token = token
        self.channel = channel
        self.onState = onState
    }

    func start() {
        isRunning = true
        generation += 1
        let g = generation
        Task { await connect(g) }
    }

    func stop() {
        isRunning = false
        generation += 1
        keepalive?.cancel()
        keepalive = nil
        task?.cancel(with: .goingAway, reason: nil)
        task = nil
        session?.invalidateAndCancel()
        session = nil
        connected = false
        onState(false)
    }

    func publish<T: Encodable>(_ value: T) async {
        guard let data = try? JSONEncoder().encode(value),
              let str = String(data: data, encoding: .utf8) else { return }
        guard connected, let task else {
            // Stash latest snapshot until the socket comes up. Only the most
            // recent matters — older queued payloads are obsolete.
            pendingPayload = str
            return
        }
        do {
            try await task.send(.string(str))
        } catch {
            // The socket is dead even if receive() hasn't noticed yet. Keep the
            // payload for the reconnect flush and go reconnect.
            pendingPayload = str
            drop(task)
        }
    }

    // ─── connection ──────────────────────────────────────────────────────────

    private func connect(_ g: Int) async {
        guard isRunning, g == generation else { return }
        guard var comps = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else { return }
        var items = comps.queryItems ?? []
        items.append(URLQueryItem(name: "token", value: token))
        items.append(URLQueryItem(name: "channel", value: channel))
        items.append(URLQueryItem(name: "role", value: "publisher"))
        comps.queryItems = items
        guard let url = comps.url else { return }

        let cfg = URLSessionConfiguration.default
        cfg.timeoutIntervalForRequest = 10
        let session = URLSession(configuration: cfg)
        let task = session.webSocketTask(with: url)

        // Wait for the WebSocket handshake to actually succeed (or fail) BEFORE
        // reporting "connected" — otherwise we blink on/off once per second
        // every time the handshake fails (wrong token, server unreachable, …).
        let delegate = WSOpenObserver()
        task.delegate = delegate
        task.resume()

        let opened = await delegate.waitForOpen()
        guard isRunning, g == generation else {
            // Stopped or superseded while the handshake was in flight.
            task.cancel(with: .goingAway, reason: nil)
            session.invalidateAndCancel()
            return
        }
        if !opened {
            task.cancel(with: .goingAway, reason: nil)
            session.invalidateAndCancel()
            connected = false
            onState(false)
            await scheduleReconnect(g)
            return
        }

        self.session?.invalidateAndCancel()
        self.session = session
        self.task = task
        connected = true
        onState(true)
        reconnectDelay = 1  // only reset after a confirmed successful open
        startKeepalive(task)

        // Flush anything that was queued while we were disconnected. This is
        // the catch-up after the start-up race and after any mid-show reconnect.
        if let pending = pendingPayload {
            pendingPayload = nil
            do { try await task.send(.string(pending)) } catch {
                pendingPayload = pending
                drop(task)
                return
            }
        }

        await readLoop(task)
    }

    private func readLoop(_ task: URLSessionWebSocketTask) async {
        while self.task === task {
            do { _ = try await task.receive() } catch { break }
        }
        drop(task)
    }

    // The one exit path for a dead connection. Idempotent per socket: the
    // keepalive, a failed send and the read loop may all notice the same
    // death, and only the first one reconnects.
    private func drop(_ dead: URLSessionWebSocketTask) {
        guard self.task === dead else { return }
        self.task = nil
        keepalive?.cancel()
        keepalive = nil
        dead.cancel(with: .goingAway, reason: nil)
        session?.invalidateAndCancel()
        session = nil
        connected = false
        onState(false)
        guard isRunning else { return }
        generation += 1
        let g = generation
        Task { await self.scheduleReconnect(g) }
    }

    private func scheduleReconnect(_ g: Int) async {
        guard isRunning, g == generation else { return }
        let delay = reconnectDelay
        reconnectDelay = min(reconnectDelay * 2, 10)
        try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        await connect(g)
    }

    // ─── keepalive ───────────────────────────────────────────────────────────

    private func startKeepalive(_ socket: URLSessionWebSocketTask) {
        keepalive?.cancel()
        keepalive = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(Self.pingInterval * 1_000_000_000))
                guard !Task.isCancelled else { return }
                let alive = await Self.ping(socket, timeout: Self.pongTimeout)
                guard !Task.isCancelled else { return }
                if !alive {
                    await self?.drop(socket)
                    return
                }
            }
        }
    }

    // True if a pong came back in time. The relay (`ws`) answers pings
    // automatically, and Nginx passes control frames through.
    private static func ping(_ socket: URLSessionWebSocketTask, timeout: TimeInterval) async -> Bool {
        await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            let once = Once()
            socket.sendPing { error in
                if once.take() { cont.resume(returning: error == nil) }
            }
            DispatchQueue.global().asyncAfter(deadline: .now() + timeout) {
                if once.take() { cont.resume(returning: false) }
            }
        }
    }
}

// A thread-safe "first caller wins" flag, so a continuation is resumed
// exactly once whichever of two callbacks fires first.
private final class Once: @unchecked Sendable {
    private let lock = NSLock()
    private var done = false
    func take() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if done { return false }
        done = true
        return true
    }
}

// MARK: - WebSocket open observer
//
// URLSessionWebSocketTask.delegate fires either `didOpenWithProtocol` on a
// successful handshake or `didCompleteWithError` on failure. We bridge that to
// a single `waitForOpen()` async call that returns true/false. URLSession holds
// the delegate alive for the task's lifetime, so it stays around to fire the
// completion callbacks even after our continuation resumes.
//
// The outcome is stored, not just signalled: if the handshake finishes before
// waitForOpen() is called, the caller still gets the real result. (It used to
// report a fast successful open as a failure.)

private final class WSOpenObserver: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
    private var continuation: CheckedContinuation<Bool, Never>?
    private var result: Bool?
    private let lock = NSLock()

    func waitForOpen() async -> Bool {
        await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            lock.lock(); defer { lock.unlock() }
            if let result { cont.resume(returning: result); return }
            continuation = cont
        }
    }

    private func resolve(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        guard result == nil else { return }
        result = value
        continuation?.resume(returning: value)
        continuation = nil
    }

    func urlSession(_ session: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didOpenWithProtocol protocol: String?) {
        resolve(true)
    }

    func urlSession(_ session: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
                    reason: Data?) {
        resolve(false)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask,
                    didCompleteWithError error: Error?) {
        resolve(false)
    }
}
