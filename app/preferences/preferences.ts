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
const validThemePreferences: ThemePreference[] = ["light", "dark", "system"];

function getSystemTheme(): "light" | "dark" {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function getValidatedThemePreference(): ThemePreference {
  const stored = window.localStorage.getItem(themeKey);
  if (stored && validThemePreferences.includes(stored as ThemePreference)) {
    return stored as ThemePreference;
  }
  return "light";
}

declare var window: any;

export default class UserPreferences {
  handlePreferencesChanged: () => void;
  private systemThemeMediaQuery: MediaQueryList | null = null;
  private boundSystemThemeHandler: (() => void) | null = null;

  constructor(handlePreferencesChanged: () => void) {
    this.handlePreferencesChanged = handlePreferencesChanged;
    this.setupSystemThemeListener();
  }

  private setupSystemThemeListener() {
    if (!window.matchMedia) return;
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = () => {
      if (this.themePreference === "system") {
        this.handlePreferencesChanged();
      }
    };
    mediaQuery.addEventListener("change", handler);
    this.systemThemeMediaQuery = mediaQuery;
    this.boundSystemThemeHandler = handler;
  }

  cleanup() {
    if (this.systemThemeMediaQuery && this.boundSystemThemeHandler) {
      this.systemThemeMediaQuery.removeEventListener("change", this.boundSystemThemeHandler);
    }
  }

  denseModeEnabled =
    viewModeKey in window.localStorage
      ? window.localStorage.getItem(viewModeKey) == denseModeValue
      : capabilities.config.defaultToDenseMode;
  lightTerminalEnabled = window.localStorage.getItem(terminalThemeKey) == terminalThemeLightValue || false;
  keyboardShortcutsEnabled = window.localStorage.getItem(keyboardShortcutsKey) === keyboardShortcutsValue;
  themePreference: ThemePreference = getValidatedThemePreference();

  get darkModeEnabled(): boolean {
    if (this.themePreference === "system") {
      return getSystemTheme() === "dark";
    }
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
