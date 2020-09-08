export const NO_BUTTON_BIT = 0;
export const LEFT_BUTTON_BIT = 1;
export const RIGHT_BUTTON_BIT = 2;
export const MIDDLE_BUTTON_BIT = 4;

export function setCursorOverride(cursor: string | null) {
  document.body.classList.toggle("cursorOverride", cursor !== null);
  document.body.style.cursor = cursor;
}
