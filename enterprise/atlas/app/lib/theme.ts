/** Light or dark, following the OS unless the viewer picked one. */
export type Theme = "system" | "light" | "dark";

const COOKIE = "atlas_theme";
const darkScheme = window.matchMedia("(prefers-color-scheme: dark)");
const listeners = new Set<() => void>();

let current: Theme = readCookie();

export function preference(): Theme {
  return current;
}

export function setPreference(theme: Theme) {
  current = theme;
  writeCookie(theme);
  apply();
}

export function isDark(): boolean {
  return current === "system" ? darkScheme.matches : current === "dark";
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

function readCookie(): Theme {
  for (const part of document.cookie.split("; ")) {
    const [name, value] = part.split("=");
    if (name === COOKIE && (value === "light" || value === "dark")) {
      return value;
    }
  }
  return "system";
}

function writeCookie(theme: Theme) {
  const attrs = ["path=/", "SameSite=Lax"];
  if (location.protocol === "https:") attrs.push("Secure");
  const domain = cookieDomain();
  if (domain) attrs.push(`domain=${domain}`);
  // "system" is the default, so it is stored as no cookie at all.
  attrs.push(theme === "system" ? "max-age=0" : `max-age=${365 * 24 * 3600}`);
  document.cookie = `${COOKIE}=${theme === "system" ? "" : theme}; ${attrs.join("; ")}`;
}

function cookieDomain(): string {
  const labels = location.hostname.split(".");
  // No domain for local testing.
  if (labels.length < 2 || /^\d+$/.test(labels[labels.length - 1])) return "";
  return labels.slice(-2).join(".");
}
