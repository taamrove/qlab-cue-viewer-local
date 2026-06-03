import Foundation

// Publishes JSON snapshots to relay-server as a "publisher" on a channel.
// Drops messages while disconnected — the relay caches the last text payload
// per channel, so a freshly-connecting subscriber always sees latest state.

actor RelayPublisher {
    private let baseURL: URL
    private let token: String
    private let channel: String
    private let onState: @Sendable (Bool) -> Void

    private var task: URLSessionWebSocketTask?
    private var session: URLSession?
    private var reconnectDelay: TimeInterval = 1
    private var isRunning = false
    private var connected = false
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
        Task { await connect() }
    }

    func stop() {
        isRunning = false
        task?.cancel(with: .goingAway, reason: nil)
        task = nil
        connected = false
        onState(false)
    }

    func publish<T: Encodable>(_ value: T) async {
        guard let data = try? JSONEncoder().encode(value),
              let str = String(data: data, encoding: .utf8) else { return }
        if connected, let task {
            try? await task.send(.string(str))
        } else {
            // Stash latest snapshot until the socket comes up. Only the most
            // recent matters — older queued payloads are obsolete.
            pendingPayload = str
        }
    }

    // ─── connection ──────────────────────────────────────────────────────────

    private func connect() async {
        guard isRunning else { return }
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
        self.session = session

        let task = session.webSocketTask(with: url)

        // Wait for the WebSocket handshake to actually succeed (or fail) BEFORE
        // reporting "connected" — otherwise we blink on/off once per second
        // every time the handshake fails (wrong token, server unreachable, …).
        let delegate = WSOpenObserver()
        task.delegate = delegate
        task.resume()

        let opened = await delegate.waitForOpen()
        if !opened || !isRunning {
            task.cancel(with: .goingAway, reason: nil)
            connected = false
            onState(false)
            if isRunning { await scheduleReconnect() }
            return
        }

        self.task = task
        connected = true
        onState(true)
        reconnectDelay = 1  // only reset after a confirmed successful open

        // Flush anything that was queued while we were disconnected. This is
        // the catch-up after the start-up race and after any mid-show reconnect.
        if let pending = pendingPayload {
            try? await task.send(.string(pending))
            pendingPayload = nil
        }

        await readLoop()
    }

    private func readLoop() async {
        guard let task else { return }
        while isRunning {
            do { _ = try await task.receive() } catch { break }
        }
        connected = false
        onState(false)
        if isRunning { await scheduleReconnect() }
    }

    private func scheduleReconnect() async {
        let delay = reconnectDelay
        reconnectDelay = min(reconnectDelay * 2, 10)
        try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        await connect()
    }
}

// MARK: - WebSocket open observer
//
// URLSessionWebSocketTask.delegate fires either `didOpenWithProtocol` on a
// successful handshake or `didCompleteWithError` on failure. We bridge that to
// a single `waitForOpen()` async call that returns true/false. URLSession holds
// the delegate alive for the task's lifetime, so it stays around to fire the
// completion callbacks even after our continuation resumes.

private final class WSOpenObserver: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
    private var continuation: CheckedContinuation<Bool, Never>?
    private var fired = false
    private let lock = NSLock()

    func waitForOpen() async -> Bool {
        await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            lock.lock(); defer { lock.unlock() }
            if fired { cont.resume(returning: false); return }
            continuation = cont
        }
    }

    private func resolve(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        guard !fired else { return }
        fired = true
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
