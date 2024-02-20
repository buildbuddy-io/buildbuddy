import React from "react";
import * as monaco from "monaco-editor";
import Select, { Option } from "../../../../app/components/select/select";

interface Props {
  editor: monaco.editor.IStandaloneCodeEditor;
}
interface State {
  versions: string[];
}

const versionsDataURL = "https://registry.build/v1/versions.json";

export default class BazelVersionSidekick extends React.Component<Props, State> {
  state = { versions: [] };

  async componentDidMount() {
    this.setState({ versions: (await (await fetch(versionsDataURL)).json()).reverse() });
  }

  render() {
    return (
      <div className="sidekick bazelversion-sidekick">
        <h1>Bazel Version</h1>
        <Select
          value={this.props.editor.getModel()?.getValue().trim()}
          onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
            this.props.editor?.executeEdits(
              null,
              [
                {
                  range: this.props.editor.getModel()?.getFullModelRange()!,
                  text: e.target.value + "\n",
                },
              ],
              [this.props.editor.getSelection()!]
            );
          }}>
          {this.state.versions.map((v) => (
            <Option>{v}</Option>
          ))}
        </Select>
      </div>
    );
  }
}
