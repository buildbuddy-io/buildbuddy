import SwiftUI
import AppKit

@main
struct BuildBuddyApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
                .background(WindowAccessor())
        }
        .windowStyle(.hiddenTitleBar)
        .commands {
            CommandGroup(replacing: .appInfo) {
                Button("About BuildBuddy") {
                    NSApplication.shared.orderFrontStandardAboutPanel(
                        options: [
                            .applicationName: "BuildBuddy",
                            .applicationVersion: "1.0.0",
                        ]
                    )
                }
            }
        }
    }
}

struct WindowAccessor: NSViewRepresentable {
    func makeNSView(context: Context) -> NSView {
        let view = NSView()
        
        DispatchQueue.main.async {
            if let window = view.window {
                // Only set these properties once, don't use toolbar
                window.titlebarAppearsTransparent = true
                window.titleVisibility = .hidden
                window.styleMask.insert(.fullSizeContentView)
                window.isMovableByWindowBackground = false // Disabled since we have custom drag area
                
                // Keep the window style consistent
                NotificationCenter.default.addObserver(
                    forName: NSWindow.didBecomeKeyNotification,
                    object: window,
                    queue: .main
                ) { _ in
                    window.titlebarAppearsTransparent = true
                    window.titleVisibility = .hidden
                }
                
                NotificationCenter.default.addObserver(
                    forName: NSWindow.didResignKeyNotification,
                    object: window,
                    queue: .main
                ) { _ in
                    window.titlebarAppearsTransparent = true
                    window.titleVisibility = .hidden
                }
            }
        }
        
        return view
    }
    
    func updateNSView(_ nsView: NSView, context: Context) {}
}
