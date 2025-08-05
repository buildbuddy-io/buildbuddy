/**
 * Definitions and utilities for working with the terminal's text content.
 *
 * Terminology used:
 *
 * - "Text" refers to the complete original text, which may contain ANSI codes,
 *   such as "\x1b[32mHELLO\x1b[m\nWORLD!"
 * - "Line" means a line from the original text, which may contain ANSI codes,
 *   such as "\x1b[32mHELLO\x1b[m". Lines are not stored with trailing newlines.
 * - "Plaintext" refers to a line with ANSI codes stripped out. This is used for
 *   searching and wrapping purposes, since ANSI codes should be ignored.
 * - "Row" refers to a wrapped line, which is rendered as a list item in the
 *   virtualized list. If wrapping is enabled, then a line can be wrapped onto
 *   multiple rows.
 */

import memoizeOne from "memoize-one";
import parseAnsi, { AnsiOffsetTag, AnsiStyle, stripAnsiCodes } from "./ansi";

/**
 * Rounding errors start messing with row positioning when there are this many
 * rows.
 */
const ROW_LIMIT = 835_000;

const TAB_STOP_WIDTH = 8;

/**
 * Contains the data needed to render the terminal text.
 */
export interface Content {
  rows: RowData[];
  matches: Match[];
}

/**
 * Data passed to the virtualized list. This is also available to each list item
 * as the "data" prop.
 */
export interface ListData {
  rows: RowData[];
  /** Text length at which a row is wrapped onto a new line. */
  rowLength: number;

  search: string;
  activeMatchIndex: number;
}

/**
 * Data needed to render a row in the virtual list.
 *
 * Note that the fine-grained line parts, such as highlighted search match
 * regions, etc. are not stored here, since that data is expensive to compute
 * and keep in memory. Instead, those are computed lazily by calling
 * `computeRows`.
 */
export interface RowData {
  /** The plaintext of the original line, with ANSI escape codes removed. */
  plaintext: string;
  /**
   * The global match index of the first match in the original line, if the line
   * contains a match; otherwise null. This is used to compute the global match
   * indexes of the match spans within the line.
   *
   * For example, if this is 7, and there are 50 matches globally, and this line
   * contains 2 matches, then the global match indexes of those 2 matches will
   * be computed as 7 and 8.
   */
  matchStartIndex: number | null;
  /**
   * The wrap offset of the line. For example, if the line wraps at 80 chars and
   * this is 1, then this represents the part of the line from index 80 to index
   * 160.
   */
  wrapOffset: number;
  /**
   * The ANSI data for this text. ANSI spans refer to the next N characters of
   * the line, tagging them with the appropriate style and/or link.
   */
  tags: AnsiOffsetTag[];
}

export interface SpanData {
  /** The plaintext in this span. */
  text: string;
  /** Parsed ANSI style. */
  style?: AnsiStyle;
  /** Parsed link, if any.  */
  link?: string;
  /**
   * If this part of the line is matched by the current search term, this will
   * be set to the match index. For example, if this is the last match out of 5
   * total matches, this will be set to 4.
   */
  matchIndex: number | null;
}

export interface Range {
  /** Start index, inclusive. */
  start: number;
  /** End index, exclusive. */
  end: number;
}

/** Region of text matched by a search. */
export interface Match {
  /**
   * The index of the row in which this match *starts* (note that a match can
   * span multiple rows due to wrapping).
   */
  rowIndex: number;
  /** Start index of this match within the original line. */
  matchIndex: number;
}

/**
 * Does a lightweight pass over the original ANSI text and splits it up into
 * lines. It also does some *light* computation to figure out how many rows
 * correspond to each line (when wrapped), as well as the row indexes of search
 * results. It does *not* do a full ANSI parse, which is too expensive. Instead,
 * ANSI parsing is done lazily, when lines are rendered.
 */
export function getContent(text: string, search: string, lineLengthLimit: number | null): Content {
  // If the line length limit is not yet known, then return empty contents,
  // since we don't yet know how to wrap the contents.
  if (lineLengthLimit === null) {
    return { rows: [], matches: [] };
  }
  const lines = text.split("\n");

  const rows: RowData[] = [];
  const matches: Match[] = [];
  let matchStartIndex = 0;
  let currentStyle: AnsiStyle = {};
  for (let line of lines) {
    // Ensure that all characters have the same visual width so that we can
    // compute wrapping more easily.
    line = normalizeSpace(line);

    const [plaintext, tags] = parseAnsi(line, currentStyle);
    const matchRanges = getMatchedRanges(plaintext, search);
    let matchIndex = 0;
    const numRowsForLine = lineLengthLimit ? Math.ceil(plaintext.length / lineLengthLimit) : 1;
    for (let i = 0; i < numRowsForLine; i++) {
      const rowEndIndex = (i + 1) * lineLengthLimit;
      while (matchRanges[matchIndex] && matchRanges[matchIndex].start < rowEndIndex) {
        const matchRange = matchRanges[matchIndex];
        matches.push({
          rowIndex: rows.length,
          matchIndex: matchRange.start,
        });
        matchIndex++;
      }
      rows.push({
        plaintext: plaintext,
        matchStartIndex: matchRanges.length ? matchStartIndex : null,
        wrapOffset: i,
        tags: tags,
      });
    }
    matchStartIndex += matchRanges.length;
  }
  return { rows: limitRows(rows), matches };
}

function limitRows(rows: RowData[]): RowData[] {
  if (rows.length < ROW_LIMIT) return rows;
  return rows.slice(-ROW_LIMIT);
}

export function normalizeSpace(text: string) {
  // Fast path for text not containing tabs.
  if (!text.includes("\t")) return text;

  // Apply tab stops: every time we encounter a tab, convert it to the number of
  // spaces required to the reach the next tab stop position. Note that tab stop
  // positions only take visible characters into account, so we have some
  // lightweight logic here to account for ANSI sequences.
  let out = "";
  let visibleLineLength = 0;
  let inAnsiSequence = false;
  for (let i = 0; i < text.length; i++) {
    if (text[i] === "\t") {
      const stop = Math.ceil((visibleLineLength + 1) / TAB_STOP_WIDTH) * TAB_STOP_WIDTH;
      while (visibleLineLength < stop) {
        out += " ";
        visibleLineLength++;
      }
      continue;
    }
    out += text[i];
    if (text[i] === "\n") {
      visibleLineLength = 0;
      inAnsiSequence = false;
      continue;
    }
    if (inAnsiSequence) {
      if (text[i] === "m") {
        inAnsiSequence = false;
      }
      continue;
    }
    if (text[i] === "\x1b" && text[i + 1] === "[") {
      inAnsiSequence = true;
      continue;
    }
    visibleLineLength++;
  }
  return out;
}

export function toPlainText(text: string) {
  return normalizeSpace(stripAnsiCodes(text));
}

export function getMatchedRanges(line: string, search: string): Range[] {
  // For now, don't support searches less than 3 chars long; they are not very
  // useful and cause jank since they often generate a huge number of matches.
  if (search.length < 3) return [];

  // Note: This logic probably doesn't work for some unicode chars
  // which have a different uppercase and lowercase length (e.g.: İ)
  search = search.toLocaleLowerCase();
  line = line.toLocaleLowerCase();
  const ranges: Range[] = [];
  let index = line.indexOf(search);
  while (index !== -1) {
    const end = index + search.length;
    ranges.push({ start: index, end });
    index = line.indexOf(search, end);
  }
  return ranges;
}

/**
 * Computes an updated active match index for when the search text changes.
 */
export function updatedMatchIndexForSearch(
  nextContent: Content,
  nextSearch: string,
  currentMatch: Match | null,
  rowRangeInView: Range | null
): number {
  if (!nextContent.matches.length) return -1;

  // If there is already an active match and it lines up with one of the new
  // matches, return the index of the match it lines up with.
  if (
    currentMatch &&
    nextContent.rows[currentMatch.rowIndex]?.plaintext
      .toLocaleLowerCase()
      .substring(currentMatch.matchIndex)
      .startsWith(nextSearch.toLocaleLowerCase())
  ) {
    for (let i = 0; i < nextContent.matches.length; i++) {
      const newMatch = nextContent.matches[i];
      if (newMatch.rowIndex === currentMatch.rowIndex && newMatch.matchIndex === currentMatch.matchIndex) {
        return i;
      }
    }
  }
  // Otherwise, try to match one of the lines that is already in view, to avoid
  // scrolling.
  if (rowRangeInView) {
    for (let i = 0; i < nextContent.matches.length; i++) {
      let match = nextContent.matches[i];
      if (match.rowIndex >= rowRangeInView.start && match.rowIndex < rowRangeInView.end) {
        return i;
      }
    }
  }

  // If all that failed, start from the top.
  return 0;
}

const BLANK_LINE_DATA: SpanData[][] = [[{ text: "", matchIndex: null }]];
/**
 * Splits an ANSI line into multiple wrapped rows, with matched ranges
 * annotated.
 */
function computeRowsImpl(
  plaintext: string,
  lengthLimit: number,
  search: string,
  matchStartIndex: number | null,
  ansiTags: AnsiOffsetTag[]
): SpanData[][] {
  if (!plaintext) return BLANK_LINE_DATA;

  // TODO: Integrate the search-matching and line-wrapping logic into the
  // parseAnsi routine to avoid the need for this extra splitting logic.

  // Build up the list of indexes at which we'll split up the ANSI parts.
  let tagOffset = 0;
  const splitIndexSet = new Set<number>();
  for (const tag of ansiTags) {
    splitIndexSet.add(tagOffset);
    tagOffset += tag.length;
  }
  let wrapOffset = 0;
  while (wrapOffset < plaintext.length) {
    splitIndexSet.add(wrapOffset);
    wrapOffset += lengthLimit;
  }
  const matches = getMatchedRanges(plaintext, search);
  for (const match of matches) {
    splitIndexSet.add(match.start);
    splitIndexSet.add(match.end);
  }
  // In case a match ends at the end of the string, make sure we don't try to
  // begin a new span at the end, since all spans should be non-empty.
  splitIndexSet.delete(plaintext.length);
  // Sort the indexes so that we can iterate in increasing order.
  const splitIndexes = [...splitIndexSet].sort((a, b) => a - b);
  // Additional safeguard against creating spans that start past the end of the
  // string. This is needed only because we don't yet handle unicode sequences
  // where the lowercase and uppercase strings have different lengths, which can
  // cause arbitrarily misaligned search ranges. It's probably better to show
  // incorrectly highlighted text in these cases rather than crashing the app.
  // See https://en.wikipedia.org/wiki/Dotted_and_dotless_I_in_computing
  while (splitIndexes.length && splitIndexes[splitIndexes.length - 1] >= plaintext.length) {
    splitIndexes.pop();
  }

  const rows: SpanData[][] = [];
  let row: SpanData[] | null = null;
  let spanTextOffset = 0;
  let tag: AnsiOffsetTag | null = null;
  let tagIndex = -1;
  let match: Range | null = null;
  let matchIndex = -1;
  for (let i = 0; i < splitIndexes.length; i++) {
    const splitIndex = splitIndexes[i];
    // Begin a new row initially or if we're at a wrapping point.
    if (!row || splitIndex % lengthLimit === 0) {
      row = [];
      rows.push(row);
    }
    // Move on to the next tag initially or if we've consumed all text from the
    // current tag.
    if (!tag || spanTextOffset === tag.length) {
      tagIndex++;
      tag = ansiTags[tagIndex];
      spanTextOffset = 0;
    }
    // Update the match initially or if we're past the current match.
    if (!match || match.end <= splitIndex) {
      matchIndex++;
      match = matches[matchIndex];
    }
    const isMatched = match && splitIndex >= match.start;
    const nextSplitIndex = splitIndexes[i + 1];
    const partLength = nextSplitIndex === undefined ? plaintext.length - splitIndex : nextSplitIndex - splitIndex;
    row.push({
      text: plaintext.substring(splitIndex, splitIndex + partLength),
      style: tag?.style,
      link: tag?.link,
      matchIndex: isMatched ? matchStartIndex! + matchIndex : null,
    });
    spanTextOffset += partLength;
  }
  return rows;
}

/**
 * Memoize up to one value for `computeRows` since it is called once for each row
 * that a line is split into when wrapped. Each call processes the entire line,
 * so this memoization prevents quadratic complexity for very long lines that
 * need to be wrapped into several rows.
 */
export const computeRows = memoizeOne(computeRowsImpl);
