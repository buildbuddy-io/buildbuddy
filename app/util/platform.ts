export function isMac(): boolean {
  return navigator.platform.startsWith("Mac");
}

export function modifierKey(): string {
  return isMac() ? "⌘" : "Ctrl";
}
