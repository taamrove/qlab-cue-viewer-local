// Drives the real RelayPublisher against a relay and logs connection state,
// publishing a counter twice a second. Built by test-reconnect.sh.
import Foundation

let url = URL(string: CommandLine.arguments[1])!
let seconds = Double(CommandLine.arguments[2]) ?? 60
let t0 = Date()
func log(_ s: String) {
    print(String(format: "%6.1fs  ", Date().timeIntervalSince(t0)) + s)
    fflush(stdout)
}

let pub = RelayPublisher(baseURL: url, token: "harness-token", channel: "reconnect-test",
                         onState: { up in log(up ? "CONNECTED" : "disconnected") })
Task {
    await pub.start()
    var n = 0
    while true {
        n += 1
        await pub.publish(["n": n])
        try? await Task.sleep(nanoseconds: 500_000_000)
    }
}
RunLoop.main.run(until: Date().addingTimeInterval(seconds))
