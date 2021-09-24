import capabilities from "../capabilities/capabilities";

const viewModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";
const comfyModeValue = "COMFY";

const terminalThemeKey = "TERMINAL_THEME";
const terminalThemeLightValue = "LIGHT";

declare var window: any;

export default class UserPreferences {
  handlePreferencesChanged: () => void;

  constructor(handlePreferencesChanged: () => void) {
    this.handlePreferencesChanged = handlePreferencesChanged;
  }

  denseModeEnabled =
    viewModeKey in window.localStorage
      ? window.localStorage.getItem(viewModeKey) == denseModeValue
      : capabilities.config.defaultToDenseMode;
  lightTerminalEnabled = window.localStorage.getItem(terminalThemeKey) == terminalThemeLightValue || false;

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
}
