import { copyToClipboard } from "../util/clipboard";
import { toPlainText } from "./text";

export function copyTerminalText(
  value: string | undefined,
  copier: (text: string) => void = copyToClipboard
): string {
  const sanitized = toPlainText(value || "");
  copier(sanitized);
  return sanitized;
}
