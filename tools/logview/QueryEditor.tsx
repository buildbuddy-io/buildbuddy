import React from "react";
import * as monaco from "monaco-editor";

export type QueryProps = {
  value: string;
  onChange: (value: string) => any;
};

export default function QueryEditor({ value, onChange }: QueryProps) {
  const root = React.useRef<HTMLDivElement>(null);
  const editor = React.useRef<monaco.editor.IStandaloneCodeEditor | null>();

  React.useEffect(() => {
    if (!root.current) return;
    const ed = (editor.current = monaco.editor.create(root.current!, {
      value,
      automaticLayout: true,
      minimap: { enabled: false },
      theme: "vs",
      language: "google-cloud-logging",
    }));
    const disposable = ed.onDidChangeModel((e) => {
      onChange(ed.getModel()?.getValue() || "");
    });
    return () => disposable.dispose();
  }, [root.current]);

  return <div className="query-editor" ref={root} />;
}

// Note: This was adapted from https://microsoft.github.io/monaco-editor/monarch.html
// It's very rough :)

const GOOGLE_CLOUD_LOGGING_LANGUAGE = {
  // Set defaultToken to 'invalid' to mark text that does not match any rules
  defaultToken: "invalid",

  operators: ["=", "<", ">", "<=", ">=", ":", "-", "AND", "OR", "NOT"],

  // we include these common regular expressions
  symbols: /[=><!~?:&|+\-*\/\^%]+/,

  // C# style strings
  escapes: /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,

  // The main tokenizer for our languages
  tokenizer: {
    root: [
      // identifiers and keywords
      [/[a-z_$][\w$]*/, { cases: { "@default": "identifier" } }],
      [/[A-Z][\w\$]*/, "type.identifier"], // to show class names nicely

      // whitespace
      { include: "@whitespace" },

      // delimiters and operators
      [/[{}()\[\]]/, "@brackets"],
      [/[<>](?!@symbols)/, "@brackets"],
      [/@symbols/, { cases: { "@operators": "operator", "@default": "" } }],

      // numbers
      [/\d*\.\d+([eE][\-+]?\d+)?/, "number.float"],
      [/0[xX][0-9a-fA-F]+/, "number.hex"],
      [/\d+/, "number"],

      // delimiter: after number because of .\d floats
      [/[;,.]/, "delimiter"],

      // strings
      [/"([^"\\]|\\.)*$/, "string.invalid"], // non-teminated string
      [/"/, { token: "string.quote", bracket: "@open", next: "@string" }],

      // characters
      [/'[^\\']'/, "string"],
      [/(')(@escapes)(')/, ["string", "string.escape", "string"]],
      [/'/, "string.invalid"],
    ],

    string: [
      [/[^\\"]+/, "string"],
      [/@escapes/, "string.escape"],
      [/\\./, "string.escape.invalid"],
      [/"/, { token: "string.quote", bracket: "@close", next: "@pop" }],
    ],

    whitespace: [
      [/[ \t\r\n]+/, "white"],
      [/\-\-.*$/, "comment"],
    ],
  },
};

// Register the language in Monaco
monaco.languages.register({ id: "google-cloud-logging" });
monaco.languages.setMonarchTokensProvider("google-cloud-logging", GOOGLE_CLOUD_LOGGING_LANGUAGE as any);
