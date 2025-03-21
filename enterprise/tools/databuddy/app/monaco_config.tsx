import * as monaco from "monaco-editor";
import { io } from "../proto/databuddy_ts_proto";

export type SchemasProvider = () => io.buildbuddy.databuddy.TableSchema[];

export const configureMonaco = (schemasProvider: SchemasProvider) => {
  self.MonacoEnvironment = {
    getWorkerUrl: function (workerId: string, label: string) {
      return "/monaco/editor.worker.js";
    },
  };
  registerSqlLanguage(schemasProvider);
};

function registerSqlLanguage(schemasProvider: SchemasProvider) {
  // TODO: get the full list from clickhouse
  const builtInFunctions = [
    "AVG",
    "CEIL",
    "COUNT",
    "FLOOR",
    "ROUND",
    "SUM",

    "addDays",
    "addHours",
    "addMinutes",
    "addSeconds",
    "countIf",
    "greatest",
    "least",
    "now",
    "quantile",
    "quantiles",
    "subtractDays",
    "subtractHours",
    "subtractMinutes",
    "subtractSeconds",
    "toDateTime",
    "toStartOfDay",
    "toUnixTimestamp",
  ];

  const sqlCompletionItemProvider: monaco.languages.CompletionItemProvider = {
    provideCompletionItems: (
      model: monaco.editor.ITextModel,
      position: monaco.Position,
      context: monaco.languages.CompletionContext,
      token: monaco.CancellationToken
    ): monaco.languages.ProviderResult<monaco.languages.CompletionList> => {
      let inFromClause = false;
      // Look back for clause
      for (let lineNumber = position.lineNumber; lineNumber >= 1; lineNumber--) {
        const line = model.getLineContent(lineNumber);
        // TODO: ignoreComments(line)
        if (!line.startsWith("--") && line.includes("FROM")) {
          inFromClause = true;
          break;
        }
      }

      // TODO: parse FROM clause
      const tableSchemas = schemasProvider();

      let options: string[] = [];
      if (inFromClause) {
        options = tableSchemas.map((table) => table.name);
      } else {
        // TODO: filter to table in FROM clause if applicable
        const fields = tableSchemas
          // .filter((schema) => schema.name === table)
          .flatMap((table) => table.columns)
          .flatMap((column) => column.name);
        options = [...fields, ...builtInFunctions];
      }

      // TODO: in GROUP BY / ORDER BY etc. limit to SELECT fields
      const fullLine = model.getLineContent(position.lineNumber);
      const lineContentBeforeCursor = fullLine.substring(0, position.column - 1);
      const tokenBeforeCursor = lineContentBeforeCursor.match(/[A-Za-z_]+$/)?.[0] ?? "";
      // TODO: also consider token after cursor?

      const suggestions: monaco.languages.CompletionItem[] = [];
      for (let i = 0; i < options.length; i++) {
        const option = options[i];
        if (!option.startsWith(tokenBeforeCursor)) {
          continue;
        }
        suggestions.push({
          label: option,
          insertText: option,
          kind: monaco.languages.CompletionItemKind.Text,
          range: {
            startLineNumber: position.lineNumber,
            startColumn: position.column - tokenBeforeCursor.length,
            endLineNumber: position.lineNumber,
            endColumn: position.column,
          },
          sortText: `${String(i).padStart(5, "0")}`,
        });
      }

      return { suggestions };
    },
  };

  monaco.languages.register({ id: "sql" });
  monaco.languages.registerCompletionItemProvider("sql", sqlCompletionItemProvider);
  monaco.languages.setMonarchTokensProvider("sql", {
    tokenizer: {
      root: [
        [
          /[A-Za-z_][\w]*/,
          {
            cases: {
              "@keywords": "keyword",
              "@builtInFunctions": "predefined",
              "@default": "identifier",
            },
          },
        ],
        [/\d+(\.\d+)?(e-?\d+)?/, "number"],
        [/[;,.]/, "delimiter"],
        [/".*?"/, "string"],
        [/'[^']*'/, "string"],
        [/`.*?`/, "string"],
        [/--.*$/, "comment"],
        [/\/\/.*$/, "comment"],
        [/<=|>=|!=|=|<|>|\+|-|\*|\//, "operator"],
      ],
    },
    keywords: [
      "AND",
      "AS",
      "ASC",
      "BY",
      "CASE",
      "CREATE",
      "DELETE",
      "DESC",
      "DISTINCT",
      "DROP",
      "ELSE",
      "END",
      "FROM",
      "GROUP",
      "HAVING",
      "IN",
      "INNER",
      "INSERT",
      "JOIN",
      "LEFT",
      "LIKE",
      "LIMIT",
      "NULL",
      "OR",
      "ORDER",
      "OUTER",
      "RIGHT",
      "SELECT",
      "TABLE",
      "THEN",
      "UPDATE",
      "WHEN",
      "WHERE",
      "WITH",
    ],
    builtInFunctions: builtInFunctions,
  });
  monaco.languages.setLanguageConfiguration("sql", {
    comments: {
      lineComment: "--",
    },
    brackets: [
      ["{", "}"],
      ["[", "]"],
      ["(", ")"],
    ],
    autoClosingPairs: [
      { open: "{", close: "}" },
      { open: "[", close: "]" },
      { open: "(", close: ")" },
      { open: '"', close: '"' },
      { open: "'", close: "'" },
    ],
  });
}
