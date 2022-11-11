export function copyToClipboard(text: string) {
  const textArea = document.createElement("textarea");
  textArea.value = text;
  Object.assign(textArea.style, {
    top: "0",
    left: "0",
    position: "fixed",
    opacity: "0",
    pointerEvents: "none",
  });
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  const success = document.execCommand("copy");
  textArea.remove();
  if (!success) {
    throw new Error("Failed to copy to clipboard.");
  }
}
