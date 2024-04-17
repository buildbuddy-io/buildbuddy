import React from "react";
import * as monaco from "monaco-editor";

import { Trash2 } from "lucide-react";
import { roundedDurationSec } from "../../../../app/format/format";
import Select, { Option } from "../../../../app/components/select/select";

interface Props {
  editor: monaco.editor.IStandaloneCodeEditor;
}

interface State {
  flags: Flag[];
  query: string;
  showModal: boolean;
  selectIndex: number;
  selectedConfig: string;
  selectedCommand: string;
}

interface Flag {
  name?: string;
  short?: string;
  type?: string;
  default?: string;
  description?: string;
  tags?: string[];

  value?: string;
  command?: string;
  config?: string;
}

const flagsDataURL = "https://registry.build/v1/flags.json";

export default class BazelrcSidekick extends React.Component<Props, State> {
  state: State = { query: "", selectedCommand: "", selectedConfig: "", showModal: false, selectIndex: 0, flags: [] };

  async componentDidMount() {
    this.setState({ flags: await (await fetch(flagsDataURL)).json() });
    document.addEventListener("keydown", this.onKeydown);
  }

  componentWillUnmount() {
    document.removeEventListener("keydown", this.onKeydown);
  }

  private onKeydown = (e: KeyboardEvent) => {
    switch (e.keyCode) {
      case 27: // Esc
        this.setState({ showModal: false });
        break;
      case 66: // Meta + B
        if (!e.metaKey) break;
        this.setState({ showModal: true, selectIndex: 0 }, () => {
          document.getElementById("flag-input")?.focus();
        });
        e.preventDefault();
        break;
    }
  };

  add(m: Flag) {
    let snippet = `${this.state.selectedCommand || "common"}${
      this.state.selectedConfig ? ":" + this.state.selectedConfig : ""
    } --${m.name?.trim()}=\n`;
    let selectedRange = this.props.editor.getSelection() || new monaco.Selection(0, 0, 0, 0);
    // By default, replace the selected range.
    let insertionRange = selectedRange;

    // We're dealing with a point selection.
    if (selectedRange.collapseToStart().equalsRange(selectedRange)) {
      let currentLine = selectedRange.endLineNumber;
      let newLine = currentLine + 1;

      // If if we're on a blank line, insert it there.
      if (this.props.editor.getModel()?.getLineMaxColumn(currentLine) == 1) {
        insertionRange = new monaco.Selection(currentLine, 0, currentLine, 0);
      } else {
        // If we're not on a blank line, insert it on the next line.
        insertionRange = new monaco.Selection(newLine, 0, newLine, 0);
        // If the next line doesn't exist yet, add it.
        if (newLine > (this.props.editor.getModel()?.getLineCount() || 0)) {
          snippet = "\n" + snippet;
        }
      }
    }

    // Apply the edits
    this.props.editor.executeEdits(null, [
      {
        range: insertionRange,
        text: snippet,
        forceMoveMarkers: true,
      },
    ]);

    // Make the edit undo-able
    this.props.editor.pushUndoStop();
    // Refocus the editor (since the edit likely came from a button click).
    this.props.editor.focus();
  }

  remove(m: RegExpMatchArray | null) {
    if (!m) {
      return;
    }
    let start = this.props.editor.getModel()?.getPositionAt(m.index || 0)!;
    let end = this.props.editor.getModel()?.getPositionAt((m.index || 0) + m[0].length)!;
    let range = new monaco.Selection(start.lineNumber, start?.column, end?.lineNumber, end.column);
    this.props.editor.executeEdits(null, [
      {
        range: range,
        text: "",
      },
    ]);
  }

  render() {
    let flagRegex = /(?<=^|\n)(?<command>[^#:\s]*?)(:(?<config>.*?))?\s+(--(?<name>.*?))(=(?<value>.*?))?(\n|$)/g;
    let selectedFlags = [...this.props.editor.getValue()?.matchAll(flagRegex)].map((f) => {
      if (f.groups?.name.startsWith("no")) {
        f.groups.name = f.groups.name.slice(2);
        f.groups.value = "false";
      }
      if (f.groups && !f.groups?.value && !f[0]?.includes("=")) {
        f.groups.value = "true";
      }
      return f;
    });

    let filteredFlags = this.state.flags
      .filter((f) => f.name?.includes(this.state.query) || f.description?.includes(this.state.query))
      .slice(0, 10);

    if (this.state.selectIndex > filteredFlags.length) {
      this.setState({ selectIndex: 0 });
    }

    let allConfigs = [...new Set(selectedFlags.map((f) => f.groups?.config)).add(this.state.selectedConfig)];
    let allCommands = [...new Set(selectedFlags.map((f) => f.groups?.command)).add(this.state.selectedCommand)];

    let matchingFlags = selectedFlags.filter(
      (m) =>
        (this.state.selectedConfig == "" || this.state.selectedConfig == m.groups?.config) &&
        (this.state.selectedCommand == "" || this.state.selectedCommand == m.groups?.command)
    );

    return (
      <div className="sidekick bazelrc-sidekick">
        <h1 className="bazelrc-header">
          Flags{" "}
          {selectedFlags.length > 0 && (
            <button
              onClick={() =>
                this.setState({ showModal: true, selectIndex: 0 }, () => {
                  document.getElementById("flag-input")?.focus();
                })
              }>
              Add Flag <span className="shortcut">(⌘ + B)</span>
            </button>
          )}
        </h1>
        <div className="bazelrc-controls">
          <Select
            value={this.state.selectedConfig}
            onChange={(e) => {
              this.setState({ selectedConfig: e.target.value == "+" ? prompt("New config:") || "" : e.target.value });
            }}>
            <Option value="">All configs</Option>
            {allConfigs
              .filter((c) => c)
              .map((c) => (
                <Option value={c}>{c}</Option>
              ))}
            <Option value="+">+ Add config</Option>
          </Select>
          <Select
            value={this.state.selectedCommand}
            onChange={(e) => {
              this.setState({ selectedCommand: e.target.value == "+" ? prompt("New command:") || "" : e.target.value });
            }}>
            <Option value="">All commands</Option>
            {allCommands
              .filter((c) => c)
              .map((c) => (
                <Option>{c}</Option>
              ))}
            <Option value="+">+ Add command</Option>
          </Select>
        </div>
        <ul>
          {matchingFlags.map((m) => {
            let matchingFlag = this.state.flags.find((flag) => flag.name == m.groups?.name);
            return (
              <Flag
                onRemove={() => this.remove(m)}
                selected={true}
                data={{
                  name: m.groups?.name,
                  value: m.groups?.value,
                  config: m.groups?.config,
                  command: m.groups?.command,
                  description: matchingFlag?.description,
                  default: matchingFlag?.default,
                  type: matchingFlag?.type,
                }}
              />
            );
          })}
        </ul>

        {selectedFlags.length == 0 && (
          <div className="bazelrc-empty-state">
            You haven't added any flags yet!
            <br />
            <br />
            <button
              onClick={() =>
                this.setState({ showModal: true, selectIndex: 0 }, () => {
                  document.getElementById("flag-input")?.focus();
                })
              }>
              Add Flag <span className="shortcut">(⌘ + B)</span>
            </button>
          </div>
        )}

        {matchingFlags.length == 0 && selectedFlags.length > 0 && (
          <div className="bazelrc-empty-state">
            No flags found in config <b>{this.state.selectedConfig}</b> for command <b>{this.state.selectedCommand}</b>!
            <br />
            <br />
            <button
              onClick={() =>
                this.setState({ showModal: true, selectIndex: 0 }, () => {
                  document.getElementById("flag-input")?.focus();
                })
              }>
              Add Flag <span className="shortcut">(⌘ + B)</span>
            </button>
          </div>
        )}

        {this.state.showModal && (
          <div
            className="flag-modal-container"
            onClick={() => {
              this.setState({ showModal: false });
            }}>
            <div className="flag-modal" onClick={(e) => e.stopPropagation()}>
              <input
                id="flag-input"
                autoComplete="off"
                value={this.state.query}
                onChange={(e) => this.setState({ query: e.target.value })}
                onKeyDown={(e) => {
                  switch (e.keyCode) {
                    case 13: //enter
                      if (filteredFlags.length == 0) break;
                      this.add(filteredFlags[this.state.selectIndex]);
                      this.setState({
                        query: "",
                        showModal: false,
                      });
                      break;
                    case 38: //up
                      let prev = Math.max(this.state.selectIndex - 1, 0);
                      this.setState({
                        selectIndex: prev,
                      });
                      document.getElementById(`option-${prev}`)?.scrollIntoView({ block: "nearest" });
                      break;
                    case 40: //down
                      let next = Math.min(this.state.selectIndex + 1, filteredFlags.length - 1);
                      this.setState({
                        selectIndex: next,
                      });
                      document.getElementById(`option-${next}`)?.scrollIntoView({ block: "nearest" });
                      break;
                  }
                }}
              />
              <div className="flag-input-options">
                {filteredFlags.map((d, index) => {
                  return (
                    <div
                      id={`option-${index}`}
                      key={d.name}
                      className={`flag-input-option ${index == this.state.selectIndex ? "selected" : ""}`}
                      style={{ cursor: "pointer" }}
                      onClick={() => {
                        this.add(d);
                        this.setState({ showModal: false });
                      }}>
                      <Flag data={d} />
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }
}

interface FlagProps {
  data: Flag;
  selected?: boolean;
  onUpdate?: () => void;
  onRemove?: () => void;
}

const Flag = (props: FlagProps) => (
  <div className="flag">
    <div className="flag-header">
      <div className="flag-name">
        {props.data.name}
        {props.selected && props.onRemove && (
          <span
            onClick={(e) => {
              props.onRemove && props.onRemove.call(this);
              e.stopPropagation();
            }}
            className="flag-remove">
            <Trash2 />
          </span>
        )}
      </div>
    </div>

    <div className="flag-properties">
      {props.data.value && <div className="flag-value">{props.data.value}</div>}
      {props.data.config && <div className="flag-config">{props.data.config}</div>}
      {props.data.command && <div className="flag-command">{props.data.command}</div>}
    </div>
    {props.data.description && <div className="flag-description">{props.data.description}</div>}
    {props.data.default && (
      <div className="flag-description">
        {props.data.type} ({props.data.default})
      </div>
    )}
  </div>
);
