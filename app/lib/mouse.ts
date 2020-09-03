import { setClassEnabled, setOrRemoveStyleProperty } from "./dom";

export const NO_BUTTON_BIT = 0;
export const LEFT_BUTTON_BIT = 1;
export const RIGHT_BUTTON_BIT = 2;
export const MIDDLE_BUTTON_BIT = 4;

export function setCursorOverride(cursor: string | null) {
  setClassEnabled(document.body, "cursorOverride", cursor !== null);
  setOrRemoveStyleProperty(document.body, "cursor", cursor, {
    important: true,
  });
}
