const ANSI_CODES_REGEX = /\x1b\[[\d;]*?m/g;

export type AnsiOffsetTag = {
  /** Length of the plaintext in this tag. */
  length: number;
  /** Parsed ANSI style. */
  style?: AnsiStyle;
  /** Parsed link, if any.  */
  link?: string;
};

export type AnsiStyle = {
  foreground?: string;
  background?: string;
  bold?: boolean;
  italic?: boolean;
  underline?: boolean;
};

const colors = ["black", "red", "green", "yellow", "blue", "magenta", "cyan", "white"];

function applyCode(style: AnsiStyle | undefined, code: number) {
  if (style === undefined) {
    return;
  }
  if (code === 0) {
    // Reset style
    delete style.foreground;
    delete style.background;
    delete style.bold;
    delete style.italic;
    delete style.underline;
    return;
  }
  if (code >= 30 && code <= 37) {
    // Foreground color
    style.foreground = colors[code - 30];
    return;
  }
  if (code === 39) {
    delete style.foreground;
    return;
  }
  if (code >= 40 && code <= 47) {
    // Background color
    style.background = colors[code - 40];
    return;
  }
  if (code === 49) {
    delete style.background;
    return;
  }
  if (code === 90) {
    // 90 is technically "bright black fg color" but just treat it as grey.
    style.foreground = "grey";
    return;
  }
  if (code === 100) {
    // 100 is technically "bright black bg color" but just treat it as grey.
    style.background = "grey";
    return;
  }
  if (code >= 90 && code <= 97) {
    // "Bright" foreground color (treat the same as non-bright for now)
    style.foreground = colors[code - 90];
    return;
  }
  if (code >= 100 && code <= 107) {
    // "Bright" background color (treat the same as non-bright for now)
    style.background = colors[code - 100];
    return;
  }

  if (code === 1) {
    style.bold = true;
    return;
  }
  if (code === 22) {
    style.bold = false;
    return;
  }
  if (code === 3) {
    style.italic = true;
    return;
  }
  if (code === 23) {
    style.italic = false;
    return;
  }
  if (code === 4) {
    style.underline = true;
    return;
  }
  if (code === 24) {
    style.underline = false;
    return;
  }
}

const cursorEscapeCharacters = [
  "A", // move cursor up
  "B", // move cursor down
  "C", // move cursor forward
  "D", // move cursor back
  "H", // move cursor to home position
  "J", // clear screen
  "K", // clear line
  "s", // save cursor position
  "u", // restore cursor position
];

// parseAnsi parses text into plaintext and offset tags that describe the ANSI
// and link properties of the plaintext. If the passed style is null or
// undefined, the tags array will be undefined.
export default function parseAnsi(text: string, style?: AnsiStyle): [string, AnsiOffsetTag[]?] {
  let plaintext: string = "";
  const tags: AnsiOffsetTag[] = [];
  let code = "";
  let tag: AnsiOffsetTag = { length: 0 };

  // rules_go produces test logs containing 0x16 ("synchronous idle") bytes at
  // the start of the line for some reason. Just ignore these for now -
  // terminals seem to ignore these bytes too.
  if (text.startsWith("\x16")) {
    text = text.substring(1);
  }

  let inEscapeSequence = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (inEscapeSequence) {
      if (char === "m") {
        // Commit the current sequence code.
        applyCode(style, Number(code || 0));
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
        applyCode(style, Number(code || 0));
        code = "";
        continue;
      }
      if (cursorEscapeCharacters.includes(char)) {
        // TODO: handle ANSI cursor escape sequences
        // For now, treat as a no-op.
        code = "";
        inEscapeSequence = false;
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
      // Begin a new tag only if the current one already has text.
      if (style !== undefined && tag.length != 0) {
        tag.style = { ...style };
        tags.push(...parseLinks(plaintext.substring(plaintext.length - tag.length, plaintext.length), tag));
        tag = { length: 0 };
      }
      continue;
    }

    // Accumulate text into the current tag.
    tag.length++;
    plaintext += char;
  }
  if (style === undefined) {
    return [plaintext, undefined];
  }
  if (tag.length != 0) {
    tag.style = { ...style };
    tags.push(...parseLinks(plaintext.substring(plaintext.length - tag.length, plaintext.length), tag));
  }
  return [plaintext, tags];
}

const LINK_REGEX =
  /(http(s)?:\/\/)(www\.)?([-a-zA-Z0-9@:%._\+~#=]{2,256})(\.[a-z]{2,6}|:[0-9]+)\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)/g;

function parseLinks(text: string, tag: AnsiOffsetTag): AnsiOffsetTag[] {
  let matches = [...text.matchAll(LINK_REGEX)];
  if (matches.length == 0) {
    return [tag];
  }
  let tags: AnsiOffsetTag[] = [];
  for (let match of matches) {
    // If there's any text before the link, create a tag for that.
    if (match.index && match.index > 0) {
      tags.push({ length: match.index, style: tag.style });
    }
    // Create a link tag for the link itself.
    tags.push({ length: match[0].length, style: { ...tag.style, underline: true }, link: match[0] });
    // If there's any text after the link, create a tag for that.
    if (match.index && match.index + match[0].length < tag.length) {
      tags.push({ length: tag.length - (match.index + match[0].length), style: tag.style });
    }
  }
  return tags;
}

/** Strips ANSI codes from text. */
export function stripAnsiCodes(text: string): string {
  return text.replace(ANSI_CODES_REGEX, "");
}
