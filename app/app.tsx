import React from "react";
import ReactDOM from "react-dom";
import * as monaco from "monaco-editor";

import RootComponent from "./root/root";

// @ts-ignore
self.MonacoEnvironment = {
  getWorkerUrl: function (workerId: string, label: string) {
    // TODO(siggisim): add language support i.e.
    //https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/basic-languages/go/go.min.js
    return `data:text/javascript;charset=utf-8,${encodeURIComponent(`
      self.MonacoEnvironment = {
        baseUrl: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/'
      };
      importScripts('https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.25.2/min/vs/base/worker/workerMain.js');`)}`;
  },
};

class Editor extends React.Component {
  private rootRef = React.createRef<HTMLDivElement>();
  private editor: monaco.editor.ICodeEditor;

  componentDidMount() {
    this.editor = monaco.editor.create(this.rootRef.current, {});
  }

  render() {
    return <div ref={this.rootRef} className="editor-root" />;
  }
}

ReactDOM.render(<Editor />, document.getElementById("app") as HTMLElement);
