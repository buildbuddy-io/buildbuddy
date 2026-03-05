import capabilities from "../capabilities/capabilities";

const viewModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";
const comfyModeValue = "COMFY";

const terminalThemeKey = "TERMINAL_THEME";
const terminalThemeLightValue = "LIGHT";

const keyboardShortcutsKey = "KEYBOARD_SHORTCUTS";
const keyboardShortcutsValue = "ENABLED";

const themeKey = "THEME";
export type ThemePreference = "light" | "dark" | "system";
const themeValues = new Set<ThemePreference>(["light", "dark", "system"]);

function readThemePreference(): ThemePreference {
  const v = window.localStorage.getItem(themeKey);
  return themeValues.has(v as ThemePreference) ? (v as ThemePreference) : "light";
}

function systemPrefersDark(): boolean {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
}

declare var window: any;

export default class UserPreferences {
  private mediaQuery = window.matchMedia?.("(prefers-color-scheme: dark)");
  private onSystemChange = () => {
    if (this.themePreference === "system") this.handlePreferencesChanged();
  };

  constructor(private handlePreferencesChanged: () => void) {
    this.mediaQuery?.addEventListener("change", this.onSystemChange);
  }

  cleanup() {
    this.mediaQuery?.removeEventListener("change", this.onSystemChange);
  }

  denseModeEnabled =
    viewModeKey in window.localStorage
      ? window.localStorage.getItem(viewModeKey) == denseModeValue
      : capabilities.config.defaultToDenseMode;
  lightTerminalEnabled = window.localStorage.getItem(terminalThemeKey) == terminalThemeLightValue || false;
  keyboardShortcutsEnabled = window.localStorage.getItem(keyboardShortcutsKey) === keyboardShortcutsValue;
  themePreference: ThemePreference = readThemePreference();

  get darkModeEnabled(): boolean {
    if (!capabilities.config.darkModeEnabled) return false;
    if (this.themePreference === "system") return systemPrefersDark();
    return this.themePreference === "dark";
  }

  toggleDenseMode() {
    this.denseModeEnabled = !this.denseModeEnabled;
    window.localStorage.setItem(viewModeKey, this.denseModeEnabled ? denseModeValue : comfyModeValue);
    this.handlePreferencesChanged();
  }

  toggleLightTerminal() {
    this.lightTerminalEnabled = !this.lightTerminalEnabled;
    window.localStorage.setItem(terminalThemeKey, this.lightTerminalEnabled ? terminalThemeLightValue : "");
    this.handlePreferencesChanged();
  }

  toggleKeyboardShortcuts() {
    this.keyboardShortcutsEnabled = !this.keyboardShortcutsEnabled;
    window.localStorage.setItem(keyboardShortcutsKey, this.keyboardShortcutsEnabled ? keyboardShortcutsValue : "");
    this.handlePreferencesChanged();
  }

  setTheme(theme: ThemePreference) {
    this.themePreference = theme;
    window.localStorage.setItem(themeKey, theme);
    this.handlePreferencesChanged();
  }
}
