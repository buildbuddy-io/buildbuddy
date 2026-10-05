/** Light or dark, following the OS unless the viewer picked one. */
export type Theme = "system" | "light" | "dark";

const KEY = "atlas.theme";
const darkScheme = window.matchMedia("(prefers-color-scheme: dark)");
const listeners = new Set<() => void>();

export function preference(): Theme {
  try {
    const v = localStorage.getItem(KEY);
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
}

export function setPreference(theme: Theme) {
  try {
    if (theme === "system") {
      localStorage.removeItem(KEY);
    } else {
      localStorage.setItem(KEY, theme);
    }
  } catch {
    // Private mode or storage disabled: the choice lasts for this page only.
  }
  apply();
}

export function isDark(): boolean {
  const p = preference();
  return p === "system" ? darkScheme.matches : p === "dark";
}

/** Puts the "dark" class on <html>, which colors.css keys its tokens on. */
export function apply() {
  document.documentElement.classList.toggle("dark", isDark());
  listeners.forEach((l) => l());
}

export function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

darkScheme.addEventListener("change", apply);
