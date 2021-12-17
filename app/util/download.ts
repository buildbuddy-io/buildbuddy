/** Downloads a string as a text file to the user's downloads directory. */
export function downloadString(content: string, fileName: string, { mimeType = "text/plain" } = {}) {
  const link = document.createElement("a");
  link.style.display = "none";
  link.setAttribute("href", `data:${mimeType};charset=utf-8,${encodeURIComponent(content)}`);
  link.setAttribute("download", fileName);
  document.body.appendChild(link);
  link.click();
  link.remove();
}
