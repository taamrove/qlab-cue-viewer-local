import Foundation
import SwiftUI

// All persisted settings, backed by UserDefaults via @AppStorage. Defaults
// match the .env.example values so first-run "just works" against a local QLab
// + the deployed relay (with a token filled in).
enum Defaults {
    static let qlabHost = "127.0.0.1"
    static let qlabPort = 53000
    static let qlabPasscode = ""
    static let relayURL = "wss://relay.trv.as"
    static let relayToken = ""
    static let relayChannel = "qlab-show-1"
    static let pollIntervalMs = 250
    static let autoConnect = true
}

final class SettingsStore: ObservableObject {
    // QLab is reached over OSC/UDP. Host + port instead of a URL because OSC
    // isn't URL-shaped and a single integer is friendlier in the UI.
    @AppStorage("qlabHost")       var qlabHost: String      = Defaults.qlabHost
    @AppStorage("qlabPort")       var qlabPort: Int         = Defaults.qlabPort
    @AppStorage("qlabPasscode")   var qlabPasscode: String  = Defaults.qlabPasscode
    @AppStorage("relayURL")       var relayURL: String      = Defaults.relayURL
    @AppStorage("relayToken")     var relayToken: String    = Defaults.relayToken
    @AppStorage("relayChannel")   var relayChannel: String  = Defaults.relayChannel
    @AppStorage("pollIntervalMs") var pollIntervalMs: Int   = Defaults.pollIntervalMs
    @AppStorage("autoConnect")    var autoConnect: Bool     = Defaults.autoConnect
}
