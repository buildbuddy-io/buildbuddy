import { FileCode, FileImageIcon, FileVideoIcon, LucideProps } from "lucide-react";
import React from "react";

import { isImageExtension, isVideoExtension, parseExtension } from "../../util/file_types";

/** Props for FileIcon. */
export interface FileIconProps extends LucideProps {
  /** File extension, including the `"."`. */
  extension: string;
}

/** Returns an appropriate icon based on the file extension. */
export function FileIcon({ extension, ...rest }: FileIconProps) {
  const ext = parseExtension(extension);
  if (isImageExtension(ext)) {
    return <FileImageIcon {...rest} />;
  }
  if (isVideoExtension(ext)) {
    return <FileVideoIcon {...rest} />;
  }
  return <FileCode {...rest} />;
}
