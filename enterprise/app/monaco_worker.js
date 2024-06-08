import * as worker from "monaco-editor/esm/vs/editor/editor.worker";

export function initialize() {
  worker.initialize();
}
