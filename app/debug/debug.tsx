import { Moon, Rows2, Rows3, Sun, Terminal } from "lucide-react";
import React from "react";
import Button from "../components/button/button";
import UserPreferences from "../preferences/preferences";

console.log("TIP: run toggleDebugMenu() to show quick preference toggles.");

const DEBUG_MENU_ENABLED_LOCALSTORAGE_KEY = "show-debug-menu";
const ENABLED_CHANGE_EVENT_NAME = "buildbuddy-debug-menu-preference-changed";

export interface DebugMenuProps {
  preferences: UserPreferences;
}

interface State {
  enabled: boolean;
}

/**
 * Renders preference toggles at the bottom-right corner of the screen. This is
 * useful for quickly testing how a page looks with different preferences
 * enabled/disabled.
 */
export default class DebugMenu extends React.Component<DebugMenuProps, State> {
  state: State = {
    enabled: isDebugMenuEnabled(),
  };

  componentDidMount() {
    window.addEventListener("storage", this.handleStorage);
    window.addEventListener(ENABLED_CHANGE_EVENT_NAME, this.handleDebugPanelPreferenceChanged);
  }

  componentWillUnmount() {
    window.removeEventListener("storage", this.handleStorage);
    window.removeEventListener(ENABLED_CHANGE_EVENT_NAME, this.handleDebugPanelPreferenceChanged);
  }

  private handleStorage = (e: StorageEvent) => {
    if (e.key === null || e.key === DEBUG_MENU_ENABLED_LOCALSTORAGE_KEY) {
      this.updateEnabledStateFromLocalStorage();
    }
  };

  private handleDebugPanelPreferenceChanged = () => {
    this.updateEnabledStateFromLocalStorage();
  };

  private updateEnabledStateFromLocalStorage() {
    const showing = isDebugMenuEnabled();
    if (showing !== this.state.enabled) {
      this.setState({ enabled: showing });
    }
  }

  render() {
    if (!this.state.enabled) {
      return null;
    }

    return (
      <div className="debug-panel" aria-label="Debug panel">
        <Button
          className="debug-panel-button icon-button"
          title={this.props.preferences.denseModeEnabled ? "Disable dense mode" : "Enable dense mode"}
          aria-label={this.props.preferences.denseModeEnabled ? "Disable dense mode" : "Enable dense mode"}
          onClick={() => this.props.preferences.toggleDenseMode()}>
          {this.props.preferences.denseModeEnabled ? <Rows2 className="icon" /> : <Rows3 className="icon" />}
        </Button>
        <Button
          className="debug-panel-button icon-button"
          title={this.props.preferences.darkModeEnabled ? "Enable light mode" : "Enable dark mode"}
          aria-label={this.props.preferences.darkModeEnabled ? "Enable light mode" : "Enable dark mode"}
          onClick={() => this.props.preferences.setTheme(this.props.preferences.darkModeEnabled ? "light" : "dark")}>
          {this.props.preferences.darkModeEnabled ? <Sun className="icon" /> : <Moon className="icon" />}
        </Button>
        <Button
          className="debug-panel-button icon-button"
          title={this.props.preferences.lightTerminalEnabled ? "Enable dark terminal" : "Enable light terminal"}
          aria-label={this.props.preferences.lightTerminalEnabled ? "Enable dark terminal" : "Enable light terminal"}
          onClick={() => this.props.preferences.toggleLightTerminal()}>
          <span className="debug-panel-terminal-icon">
            <Terminal className="icon debug-panel-terminal-base-icon" />
            {this.props.preferences.lightTerminalEnabled ? (
              <Moon className="icon debug-panel-terminal-theme-icon" />
            ) : (
              <Sun className="icon debug-panel-terminal-theme-icon" />
            )}
          </span>
        </Button>
      </div>
    );
  }
}

function isDebugMenuEnabled() {
  return window.localStorage.getItem(DEBUG_MENU_ENABLED_LOCALSTORAGE_KEY) === String(true);
}

(window as any).toggleDebugMenu = () => {
  window.localStorage.setItem(DEBUG_MENU_ENABLED_LOCALSTORAGE_KEY, String(!isDebugMenuEnabled()));
  window.dispatchEvent(new Event(ENABLED_CHANGE_EVENT_NAME));
};
