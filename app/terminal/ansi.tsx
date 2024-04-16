const ANSI_CODES_REGEX = /\x1b\[[\d;]*?m/g;

export type AnsiTextSpan = {
  /** Text in this span, with ANSI escape sequences removed. */
  text: string;
  /** Parsed ANSI style. */
  style: AnsiStyle;
  /** Parsed link, if any.  */
  link: string;
};

type AnsiStyle = {
  foreground?: string;
  background?: string;
  bold?: boolean;
  italic?: boolean;
  underline?: boolean;
};

const colors = ["black", "red", "green", "yellow", "blue", "magenta", "cyan", "white"];

function applyCode(span: AnsiTextSpan, code: number) {
  if (code === 0) {
    // Reset style
    span.style = {};
    return;
  }
  if (code >= 30 && code <= 37) {
    // Foreground color
    span.style.foreground = colors[code - 30];
    return;
  }
  if (code === 39) {
    delete span.style.foreground;
    return;
  }
  if (code >= 40 && code <= 47) {
    // Background color
    span.style.background = colors[code - 40];
    return;
  }
  if (code === 49) {
    delete span.style.background;
    return;
  }
  if (code === 90) {
    // 90 is technically "bright black fg color" but just treat it as grey.
    span.style.foreground = "grey";
    return;
  }
  if (code === 100) {
    // 100 is technically "bright black bg color" but just treat it as grey.
    span.style.background = "grey";
    return;
  }
  if (code >= 90 && code <= 97) {
    // "Bright" foreground color (treat the same as non-bright for now)
    span.style.foreground = colors[code - 90];
    return;
  }
  if (code >= 100 && code <= 107) {
    // "Bright" background color (treat the same as non-bright for now)
    span.style.background = colors[code - 100];
    return;
  }

  if (code === 1) {
    span.style.bold = true;
    return;
  }
  if (code === 22) {
    span.style.bold = false;
    return;
  }
  if (code === 3) {
    span.style.italic = true;
    return;
  }
  if (code === 23) {
    span.style.italic = false;
    return;
  }
  if (code === 4) {
    span.style.underline = true;
    return;
  }
  if (code === 24) {
    span.style.underline = false;
    return;
  }
}

export default function parseAnsi(text: string): AnsiTextSpan[] {
  let span = { text: "", style: {}, link: "" };
  const spans: AnsiTextSpan[] = [];
  let code = "";

  let inEscapeSequence = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (inEscapeSequence) {
      if (char === "m") {
        // Commit the current sequence code.
        applyCode(span, Number(code || 0));
        code = "";
        // Escape sequence has ended.
        inEscapeSequence = false;
        continue;
      }
      if (char >= "0" && char <= "9") {
        // Accumulate sequence code.
        code += char;
        continue;
      }
      if (char === ";") {
        // Commit the current sequence code.
        applyCode(span, Number(code || 0));
        code = "";
        continue;
      }
      // Unexpected character.
      continue;
    }

    if (char === "\x1b") {
      const nextChar = text[i + 1];
      if (nextChar !== "[") {
        continue;
      }
      inEscapeSequence = true;
      // Skip the "[" on the next iteration, to effectively consume the start
      // sequence "\x1b[" as a logical unit.
      i++;
      // Begin a new span only if the current one already has text.
      // Preserve styles from the current span.
      if (span.text) {
        span = { text: "", style: { ...span.style }, link: "" };
      }
      continue;
    }

    // Accumulate text into the current span.
    span.text += char;
    if (spans[spans.length - 1] !== span) {
      spans.push(span);
    }
  }
  let linkSpans = [];
  for (let span of spans) {
    linkSpans.push(...parseLinks(span));
  }
  return linkSpans;
}

const LINK_REGEX = /(http(s)?:\/\/)?(www\.)?([-a-zA-Z0-9@:%._\+~#=]{2,256})(\.[a-z]{2,6}|:[0-9]+)\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)/g;

function parseLinks(span: AnsiTextSpan): AnsiTextSpan[] {
  let matches = [...span.text.matchAll(LINK_REGEX)];
  if (matches.length == 0) {
    return [span];
  }
  let spans = [];
  for (let match of matches) {
    // If there's any text before the link, create a span for that.
    if (match.index && match.index > 0) {
      spans.push({ text: span.text.substr(0, match.index), style: span.style, link: "" });
    }
    // Create a link span for the link itself.
    spans.push({ text: match[0], style: { ...span.style, underline: true }, link: match[0] });
    // If there's any text after the link, create a span for that.
    if (match.index && match.index + match[0].length < span.text.length) {
      spans.push({ text: span.text.substr(match.index + match[0].length), style: span.style, link: "" });
    }
  }
  return spans;
}

/** Strips ANSI codes from text. */
export function stripAnsiCodes(text: string): string {
  return text.replace(ANSI_CODES_REGEX, "");
}
