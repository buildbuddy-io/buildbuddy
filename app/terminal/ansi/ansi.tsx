const foregroundColors: Record<string, string> = {
  "30": "black",
  "31": "red",
  "32": "green",
  "33": "yellow",
  "34": "blue",
  "35": "magenta",
  "36": "cyan",
  "37": "white",
  "90": "grey",
};

const backgroundColors: Record<string, string> = {
  "40": "black",
  "41": "red",
  "42": "green",
  "43": "yellow",
  "44": "blue",
  "45": "magenta",
  "46": "cyan",
  "47": "white",
  "100": "grey",
};

type StyleType = "bold" | "italic" | "underline";

const styles: Record<string, StyleType> = {
  "1": "bold",
  "3": "italic",
  "4": "underline",
};

export type AnsiTextSpan = {
  text: string;
  style?: {
    background?: string;
    foreground?: string;

    bold?: boolean;
    italic?: boolean;
    underline?: boolean;
  };
};

export default function parse(str: string): AnsiTextSpan[] {
  let matchingControl = null;
  let matchingData: string | null = null;
  let matchingText = "";
  let ansiState: string[] = [];
  let span: Partial<AnsiTextSpan> = {};
  let result: AnsiTextSpan[] = [];

  for (let i = 0; i < str.length; i++) {
    if (matchingControl !== null) {
      if (matchingControl === "\x1b" && str[i] === "[") {
        if (matchingText) {
          span.text = matchingText;
          result.push(span as AnsiTextSpan);
          span = {};
          matchingText = "";
        }

        matchingControl = null;
        matchingData = "";
      } else {
        matchingText += matchingControl + str[i];
        matchingControl = null;
      }

      continue;
    } else if (matchingData !== null) {
      if (str[i] === ";") {
        ansiState.push(matchingData);
        matchingData = "";
      } else if (str[i] === "m") {
        ansiState.push(matchingData);
        matchingData = null;
        matchingText = "";

        for (let a = 0; a < ansiState.length; a++) {
          const ansiCode = Number(ansiState[a]);

          if (foregroundColors[ansiCode]) {
            span.style ||= {};
            span.style.foreground = foregroundColors[ansiCode];
          } else if (backgroundColors[ansiCode]) {
            span.style ||= {};
            span.style.background = backgroundColors[ansiCode];
          } else if (ansiCode === 39) {
            const style = span.style;
            if (style) delete style.foreground;
          } else if (ansiCode === 49) {
            const style = span.style;
            if (style) delete style.background;
          } else if (styles[ansiCode]) {
            span.style ||= {};
            span.style[styles[ansiCode]] = true;
          } else if (ansiCode === 22) {
            span.style ||= {};
            span.style.bold = false;
          } else if (ansiCode === 23) {
            span.style ||= {};
            span.style.italic = false;
          } else if (ansiCode === 24) {
            span.style ||= {};
            span.style.underline = false;
          }
        }

        ansiState = [];
      } else {
        matchingData += str[i];
      }

      continue;
    }

    if (str[i] === "\x1b") {
      matchingControl = str[i];
    } else {
      matchingText += str[i];
    }
  }

  if (matchingText) {
    span.text = matchingText + (matchingControl || "");
    result.push(span as AnsiTextSpan);
  }

  return result;
}
