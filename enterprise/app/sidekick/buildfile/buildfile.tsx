import React from "react";
import * as monaco from "monaco-editor";
import { PlayCircle } from "lucide-react";

interface Props {
  editor: monaco.editor.IStandaloneCodeEditor;
  onBazelCommand: (c: string) => void;
}
interface State {}

const commands = ["build", "test", "run"];
export default class BuildFileSidekick extends React.Component<Props, State> {
  state = {};

  render() {
    // TODO(siggisim): this hueristic for finding target names is janky, do something more clever here.
    let targetNames = this.props.editor.getValue().matchAll(/\bname\s*=\s*"(?<name>[^"]*)"/g);
    let path = this.getBuildPath(this.props.editor.getModel()?.uri.path || "");
    return (
      <div className="sidekick buildfile-sidekick">
        <h1>Targets</h1>
        {[...targetNames].map((t) => {
          return (
            <ul className="buildfile-command-group">
              {commands.map((c) => {
                let bazelCommand = `${c} ${path}:${t.groups?.name}`;
                return (
                  <li>
                    <button className="buildfile-command-item" onClick={() => this.props.onBazelCommand(bazelCommand)}>
                      <PlayCircle /> bazel {bazelCommand}
                    </button>
                  </li>
                );
              })}
            </ul>
          );
        })}
      </div>
    );
  }

  getBuildPath(path: string) {
    let lastSlashIndex = path.lastIndexOf("/");
    if (lastSlashIndex == -1) {
      lastSlashIndex = 0;
    }
    let firstSlashIndex = 0;
    if (path.startsWith("/")) {
      firstSlashIndex = 1;
    }
    return path.substr(firstSlashIndex, lastSlashIndex - 1);
  }
}
