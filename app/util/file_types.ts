/**
 * Parses the file extension from the given file name. Returns the extension,
 * including the `"."`. Returns `""` if no extension is detected.
 */
export function parseExtension(filename: string): string {
  const index = filename.lastIndexOf(".");
  if (index === -1) {
    return "";
  }
  return filename.slice(index);
}

export function isImageExtension(extension: string): boolean {
  switch (extension.toLowerCase()) {
    case ".bmp":
    case ".gif":
    case ".ico":
    case ".jpg":
    case ".jpeg":
    case ".png":
    case ".svg":
    case ".webp":
      return true;
    default:
      return false;
  }
}

export function isVideoExtension(extension: string): boolean {
  switch (extension.toLowerCase()) {
    case ".webm":
    case ".mp4":
    case ".mov":
    case ".avi":
    case ".mkv":
    case ".wmv":
    case ".flv":
    case ".m4v":
      return true;
    default:
      return false;
  }
}
