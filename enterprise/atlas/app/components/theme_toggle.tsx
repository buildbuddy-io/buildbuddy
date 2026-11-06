import { Monitor, Moon, Sun } from "lucide-react";
import React from "react";
import * as theme from "../lib/theme";

const ORDER: theme.Theme[] = ["system", "light", "dark"];

/** Cycles the theme: follow the OS, light, dark. */
export function ThemeToggle() {
  const [current, setCurrent] = React.useState(theme.preference());
  React.useEffect(() => theme.subscribe(() => setCurrent(theme.preference())), []);
  const next = ORDER[(ORDER.indexOf(current) + 1) % ORDER.length];
  const Icon = current === "light" ? Sun : current === "dark" ? Moon : Monitor;
  return (
    <button
      className="atlas-theme-toggle"
      title={`Theme: ${current}. Click for ${next}.`}
      onClick={() => theme.setPreference(next)}>
      <Icon className="icon" />
    </button>
  );
}

export default ThemeToggle;
