// Prints how many menu bar status items are on screen right now.
//
// `NSStatusItem.isVisible` cannot answer this: it reports true while macOS 26
// is not displaying the item (#6093). The window server can. On macOS 15 a
// status item is a window owned by its app, and on macOS 26 one owned by
// Control Center, so match on the level and name rather than the owner. Only
// an item that is actually displayed has kCGWindowIsOnscreen set. The window
// level and name are not public API, and reading another app's window name
// needs Screen Recording, which hosted runners have. If any of that changes the
// count stops rising and the check goes red, so it cannot pass by accident.
import CoreGraphics
import Foundation

let statusWindowLevel = 25

guard let windows = CGWindowListCopyWindowInfo([.optionAll], kCGNullWindowID) as? [[String: Any]]
else {
    FileHandle.standardError.write(Data("window list unavailable\n".utf8))
    exit(1)
}

let shown = windows.filter { window in
    window[kCGWindowLayer as String] as? Int == statusWindowLevel
        && window[kCGWindowIsOnscreen as String] as? Bool == true
        && (window[kCGWindowName as String] as? String)?.hasPrefix("Item-") == true
}
print(shown.count)
