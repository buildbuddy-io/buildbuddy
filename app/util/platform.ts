export function isMac() {
  return navigator.platform.startsWith("Mac");
}

export function modifierKey() {
  return isMac() ? "⌘" : "Ctrl";
}
