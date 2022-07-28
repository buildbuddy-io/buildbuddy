import React from "react";
import { ListChildComponentProps } from "react-window";
import { ListData, SpanData, computeRows } from "./text";

export const ROW_HEIGHT_PX = 20;

/**
 * Renders a single row in the terminal. A row may be a complete line, or a part
 * of a wrapped line (if wrapping is enabled).
 */
export function Row({ data, index, style }: ListChildComponentProps<ListData>) {
  const rowData = data.rows[index];
  // Use the memoized version of getRows(), since we'll call it several times in
  // quick succession when rendering each row of a wrapped line.
  const rowsForLine = computeRows(rowData.line, data.rowLength, data.search, rowData.matchStartIndex);
  const row = rowsForLine[rowData.wrapOffset];
  if (!row) {
    console.error("Row mismatch:", { rowData, rowsForLine });
    return null;
  }
  return (
    <div
      style={{
        ...style,
        // Set line-height to match row height so that selection highlights
        // are sized properly.
        lineHeight: `${ROW_HEIGHT_PX}px`,
      }}
      className="terminal-line">
      {row.map((part, i) => (
        <RowSpan key={i} {...part} isActiveMatch={part.matchIndex === data.activeMatchIndex} />
      ))}
      {row === rowsForLine[rowsForLine.length - 1] && "\n"}
    </div>
  );
}

interface RowSpanProps extends SpanData {
  isActiveMatch: boolean;
}

/**
 * Renders a `<span>` with one or more ANSI styles applied to the whole span.
 */
function RowSpan({ text, matchIndex, isActiveMatch, style }: RowSpanProps) {
  if (!style) style = {};
  const className = [
    style.background && `ansi-bg-${style.background}`,
    style.foreground && `ansi-fg-${style.foreground}`,
    style.bold && "ansi-bold",
    style.italic && "ansi-italic",
    style.underline && "ansi-underline",
    matchIndex !== null && "search-match",
    isActiveMatch && "active-search-match",
  ]
    .filter(Boolean)
    .join(" ");
  return <span className={className}>{text}</span>;
}
