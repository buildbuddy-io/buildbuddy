import { Check, Copy } from "lucide-react";
import React from "react";
import { copyToClipboard } from "../../../../app/util/clipboard";

export type CopyButtonProps = {
  text: string;
  /** Just the icon, for tight spots like a port chip. */
  compact?: boolean;
  className?: string;
};

/** Copies text to the clipboard and says so for a moment. */
export function CopyButton({ text, compact, className = "" }: CopyButtonProps) {
  const [copied, setCopied] = React.useState(false);
  return (
    <button
      className={`atlas-copy-button ${copied ? "copied" : ""} ${compact ? "compact" : ""} ${className}`}
      title={compact ? `copy ${text}` : undefined}
      onClick={(e) => {
        e.preventDefault();
        try {
          copyToClipboard(text);
        } catch {
          return;
        }
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}>
      {copied ? <Check className="icon" /> : <Copy className="icon" />}
      {!compact && (copied ? "copied" : "copy")}
    </button>
  );
}

export default CopyButton;
