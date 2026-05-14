import { copyToClipboard } from "../util/clipboard";
import { toPlainText } from "./text";

export function copyTerminalText(value: string | undefined, copier: (text: string) => void = copyToClipboard) {
  const sanitized = toPlainText(value || "");
  copier(sanitized);
  return sanitized;
}
