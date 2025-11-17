import capabilities from "../capabilities/capabilities";

const viewModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";
const comfyModeValue = "COMFY";

const terminalThemeKey = "TERMINAL_THEME";
const terminalThemeLightValue = "LIGHT";

const keyboardShortcutsKey = "KEYBOARD_SHORTCUTS";
const keyboardShortcutsValue = "ENABLED";

declare var window: any;

export default class UserPreferences {
  handlePreferencesChanged: () => void;

  constructor(handlePreferencesChanged: () => void) {
    this.handlePreferencesChanged = handlePreferencesChanged;
  }

  denseModeEnabled: boolean =
    viewModeKey in window.localStorage
      ? window.localStorage.getItem(viewModeKey) == denseModeValue
      : capabilities.config.defaultToDenseMode;
  lightTerminalEnabled: boolean = window.localStorage.getItem(terminalThemeKey) == terminalThemeLightValue || false;
  keyboardShortcutsEnabled: boolean = window.localStorage.getItem(keyboardShortcutsKey) === keyboardShortcutsValue;

  toggleDenseMode(): void {
    this.denseModeEnabled = !this.denseModeEnabled;
    window.localStorage.setItem(viewModeKey, this.denseModeEnabled ? denseModeValue : comfyModeValue);
    this.handlePreferencesChanged();
  }

  toggleLightTerminal(): void {
    this.lightTerminalEnabled = !this.lightTerminalEnabled;
    window.localStorage.setItem(terminalThemeKey, this.lightTerminalEnabled ? terminalThemeLightValue : "");
    this.handlePreferencesChanged();
  }

  toggleKeyboardShortcuts(): void {
    this.keyboardShortcutsEnabled = !this.keyboardShortcutsEnabled;
    window.localStorage.setItem(keyboardShortcutsKey, this.keyboardShortcutsEnabled ? keyboardShortcutsValue : "");
    this.handlePreferencesChanged();
  }
}
