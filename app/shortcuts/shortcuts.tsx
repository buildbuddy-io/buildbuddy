import UserPreferences from "../preferences/preferences";
import { v4 as uuid } from "uuid";

export class KeyCombo {
  public static a = new KeyCombo(false, "KeyA");
  public static b = new KeyCombo(false, "KeyB");
  public static c = new KeyCombo(false, "KeyC");
  public static d = new KeyCombo(false, "KeyD");
  public static e = new KeyCombo(false, "KeyE");
  public static f = new KeyCombo(false, "KeyF");
  public static g = new KeyCombo(false, "KeyG");
  public static h = new KeyCombo(false, "KeyH");
  public static i = new KeyCombo(false, "KeyI");
  public static j = new KeyCombo(false, "KeyJ");
  public static k = new KeyCombo(false, "KeyK");
  public static l = new KeyCombo(false, "KeyL");
  public static m = new KeyCombo(false, "KeyM");
  public static n = new KeyCombo(false, "KeyN");
  public static o = new KeyCombo(false, "KeyO");
  public static p = new KeyCombo(false, "KeyP");
  public static q = new KeyCombo(false, "KeyQ");
  public static r = new KeyCombo(false, "KeyR");
  public static s = new KeyCombo(false, "KeyS");
  public static t = new KeyCombo(false, "KeyT");
  public static u = new KeyCombo(false, "KeyU");
  public static v = new KeyCombo(false, "KeyV");
  public static w = new KeyCombo(false, "KeyW");
  public static x = new KeyCombo(false, "KeyX");
  public static y = new KeyCombo(false, "KeyY");
  public static z = new KeyCombo(false, "KeyZ");
  public static shift_c = new KeyCombo(true, "KeyC");
  public static question = new KeyCombo(true, "Slash");
  public static esc = new KeyCombo(false, "Escape");
  public static enter = new KeyCombo(false, "Enter");

  // Add other modifiers here as needed.
  shiftKey: boolean;
  code: string;

  constructor(shiftKey: boolean, code: string) {
    this.shiftKey = shiftKey;
    this.code = code;
  }

  matches(event: KeyboardEvent): boolean {
    return event.shiftKey == this.shiftKey && event.code === this.code;
  }

  isEqual(other: KeyCombo): boolean {
    return this.shiftKey == other.shiftKey && this.code === other.code;
  }

  toString(): string {
    if (this.shiftKey) {
      return "shift-" + this.code;
    } else {
      return this.code;
    }
  }
}

class Shortcut {
  keyCombo: KeyCombo[];
  keyComboPosition = 0;
  action: () => void;

  constructor(keyCombo: KeyCombo[], action: () => void) {
    this.keyCombo = keyCombo;
    this.action = action;
  }

  reset() {
    this.keyComboPosition = 0;
  }

  matchKeyboardEvent(event: KeyboardEvent) {
    if (this.keyCombo[this.keyComboPosition].matches(event)) {
      this.keyComboPosition++;
    } else {
      this.keyComboPosition = 0;
    }
    if (this.keyComboPosition == this.keyCombo.length) {
      this.action();
      this.keyComboPosition = 0;
    }
  }

  // Returns true if this keyboard shortcut "collides" with the provided
  // keyboard shortcut. Collides means that one of the shortcut keystroke
  // combinations is a prefix of the other, so it will always be activated
  // when the longer one is entered.
  // TODO(iain): this check is not comprehensive, we should check for
  // collisions within another prefix, e.g. 'a' should collide with 'g-a'.
  collidesWith(other: Shortcut): boolean {
    let len = Math.min(this.keyCombo.length, other.keyCombo.length);
    for (let i = 0; i < len; i++) {
      if (!this.keyCombo[i].isEqual(other.keyCombo[i])) {
        return false;
      }
    }
    return true;
  }
}

export class Shortcuts {
  shortcuts?: Map<string, Shortcut> = undefined;
  preferences?: UserPreferences = undefined;

  public setPreferences(preferences: UserPreferences) {
    this.preferences = preferences;
  }

  // Registers the provided keyboard combination + action as a shortcut.
  // Returns a handle that may be used to deregister this shortcut when it is
  // no longer invokable. This function will throw an error if the same
  // shortcut is registered multiple times, so if the calling component will
  // unmount and remount, shortcuts should be deregistered and reregistered.
  //
  // IMPORTANT: usually you'll want to register shortcuts in componentDidMount
  // not componentWillMount because componentWillMount can be called when
  // another component is still mounted, causing shortcut conflicts.
  public register(keyCombo: KeyCombo, action: () => void): string {
    return this.registerSequence([keyCombo], action);
  }

  // Registers the provided keyboard combination sequence + action as a
  // shortcut. Returns a handle that may be used to deregister this shortcut
  // when it is no longer invokable. This function will throw an error if the
  // same shortcut is registered multiple times, so if the calling component
  // will unmount and remount, shortcuts should be deregistered and
  // reregistered.
  //
  // IMPORTANT: usually you'll want to register shortcuts in componentDidMount
  // not componentWillMount because componentWillMount can be called when
  // another component is still mounted, causing shortcut conflicts.
  public registerSequence(keyCombo: KeyCombo[], action: () => void): string {
    if (!this.shortcuts) {
      this.shortcuts = new Map<string, Shortcut>();
      document.addEventListener("keydown", (event: KeyboardEvent) => {
        if (!this.preferences?.keyboardShortcutsEnabled) {
          // TODO(iain): instead of reset-alling on keypress, do it on
          // preference change. Note that this will require breaking the
          // cyclical dependency between shortcuts & preferences.
          this.resetAll();
          return;
        }
        // Don't run when typing into a text box.
        let activeElement = document.activeElement as HTMLInputElement;
        if (
          (activeElement.tagName === "INPUT" && activeElement.type === "text") ||
          activeElement.tagName === "TEXTAREA"
        ) {
          this.resetAll();
          return;
        }
        for (let shortcut of this.shortcuts?.values() || []) {
          shortcut.matchKeyboardEvent(event);
        }
      });
    }

    let handle = uuid();
    let shortcut = new Shortcut(keyCombo, action);
    for (let otherShortcut of this.shortcuts.values()) {
      if (shortcut.collidesWith(otherShortcut)) {
        // TODO(iain): figure out a way to be notified of these errors.
        // --> https://github.com/buildbuddy-io/buildbuddy-internal/issues/2242
        console.warn("Duplicate keyboard shortcut registered: " + shortcut.keyCombo);
      }
    }
    this.shortcuts.set(handle, shortcut);
    return handle;
  }

  resetAll() {
    for (let shortcut of this.shortcuts?.values() || []) {
      shortcut.reset();
    }
  }

  // Deregisters the keyboard shortcut with the provided handle. If the
  // provided keyboard shortcut handle is unrecognized, this function
  // silently returns.
  public deregister(handle: string) {
    this.shortcuts?.delete(handle);
  }
}

export default new Shortcuts();
