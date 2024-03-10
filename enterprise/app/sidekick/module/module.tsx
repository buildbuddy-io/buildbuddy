import React from "react";
import * as monaco from "monaco-editor";

import { ArrowUp, GitCommit, Github, HistoryIcon, Star, Trash2 } from "lucide-react";
import { roundedDurationSec } from "../../../../app/format/format";

interface Props {
  editor: monaco.editor.IStandaloneCodeEditor;
}

interface State {
  modules: Module[];
  query: string;
  showModal: boolean;
  selectIndex: number;
}

interface Module {
  name: string;
  module_snippet?: string;
  modules: { name: string }[];
  repo?: { full_name: string; stargazers_count: number; description: string; owner: { avatar_url: string } };
  releases: { tag_name?: string; published_at?: string }[];
}

const modulesDataURL = "https://registry.build/v1/modules.json";

export default class ModuleSidekick extends React.Component<Props, State> {
  state: State = { query: "", showModal: false, selectIndex: 0, modules: [] };

  async componentDidMount() {
    this.setState({ modules: await (await fetch(modulesDataURL)).json() });

    document.onkeydown = (e) => {
      switch (e.keyCode) {
        case 27: // Esc
          this.setState({ showModal: false });
          break;
        case 66: // Meta + B
          if (!e.metaKey) break;
          this.setState({ showModal: true, selectIndex: 0 }, () => {
            document.getElementById("module-input")?.focus();
          });
          e.preventDefault();
          break;
      }
    };
  }

  add(m: Module) {
    let snippet = m.module_snippet?.trim() + "\n" || "unknown";
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

  update(match: RegExpMatchArray, module: Module) {
    let start = this.props.editor.getModel()?.getPositionAt(match.index || 0)!;
    let end = this.props.editor.getModel()?.getPositionAt((match.index || 0) + match[0].length)!;
    let range = new monaco.Selection(start.lineNumber, start?.column, end?.lineNumber, end.column);

    this.props.editor.executeEdits(null, [
      {
        range: range,
        text: module.module_snippet?.trim() + "\n" || "unknown",
      },
    ]);
  }

  selected(selectedDeps: Array<RegExpMatchArray>, name: string) {
    for (let d of selectedDeps) {
      if (d.groups?.name == name) {
        return d;
      }
    }
    return null;
  }

  render() {
    let moduleRegex = /bazel_dep\(.*?name\s=\s"(?<name>.*?)".*?version\s=\s"(?<version>.*?)".*?\)\n?/g;
    let selectedDeps = [...this.props.editor.getValue()?.matchAll(moduleRegex)];
    let filteredDeps = this.state.modules
      .filter(
        (d) =>
          d.modules.length > 0 &&
          d.name?.includes(this.state.query) &&
          (this.state.query != "" || !selectedDeps.find((s) => d.modules.find((m) => m.name == s.groups?.name)))
      )
      .sort(popularity)
      .slice(0, 10);

    if (this.state.selectIndex > filteredDeps.length) {
      this.setState({ selectIndex: 0 });
    }

    return (
      <div className="sidekick modules-sidekick">
        <h1 className="modules-header">
          Modules{" "}
          {selectedDeps.length > 0 && (
            <button
              onClick={() =>
                this.setState({ showModal: true, selectIndex: 0 }, () => {
                  document.getElementById("module-input")?.focus();
                })
              }>
              Add Module <span className="shortcut">(⌘ + B)</span>
            </button>
          )}
        </h1>
        <ul>
          {selectedDeps.map((m) => {
            let matchingModule = this.state.modules.find((module) =>
              module.modules.find((mm: any) => mm.name == m.groups?.name)
            );
            let latestMatch = [...(matchingModule?.module_snippet?.matchAll(moduleRegex) || [])].pop();
            if (matchingModule) {
              return (
                <Module
                  onRemove={() => this.remove(m)}
                  onUpdate={
                    (latestMatch?.groups?.version &&
                      m.groups?.version != latestMatch?.groups?.version &&
                      (() => this.update(m, matchingModule!))) ||
                    undefined
                  }
                  selected={true}
                  data={matchingModule}
                />
              );
            }
            return (
              <Module
                onRemove={() => this.remove(m)}
                selected={true}
                data={{ name: m.groups?.name || "", releases: [{ tag_name: m.groups?.version }], modules: [] }}
              />
            );
          })}
        </ul>

        {selectedDeps.length == 0 && (
          <div className="modules-empty-state">
            You haven't added any modules yet!
            <br />
            <br />
            <button
              onClick={() =>
                this.setState({ showModal: true, selectIndex: 0 }, () => {
                  document.getElementById("module-input")?.focus();
                })
              }>
              Add Module <span className="shortcut">(⌘ + B)</span>
            </button>
          </div>
        )}

        {this.state.showModal && (
          <div
            className="module-modal-container"
            onClick={() => {
              this.setState({ showModal: false });
            }}>
            <div className="module-modal" onClick={(e) => e.stopPropagation()}>
              <input
                id="module-input"
                autoComplete="off"
                value={this.state.query}
                onChange={(e) => this.setState({ query: e.target.value })}
                onKeyDown={(e) => {
                  switch (e.keyCode) {
                    case 13: //enter
                      if (filteredDeps.length == 0) break;
                      this.add(filteredDeps[this.state.selectIndex]);
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
                      let next = Math.min(this.state.selectIndex + 1, filteredDeps.length - 1);
                      this.setState({
                        selectIndex: next,
                      });
                      document.getElementById(`option-${next}`)?.scrollIntoView({ block: "nearest" });
                      break;
                  }
                }}
              />
              <div className="module-input-options">
                {filteredDeps.sort(popularity).map((d, index) => {
                  let selected = this.selected(selectedDeps, d.name);
                  return (
                    <div
                      id={`option-${index}`}
                      key={d.repo?.full_name}
                      className={`module-input-option ${index == this.state.selectIndex ? "selected" : ""}`}
                      style={{ cursor: "pointer" }}
                      onClick={() => {
                        this.add(d);
                        this.setState({ showModal: false });
                      }}>
                      <Module onRemove={() => this.remove(selected)} selected={Boolean(selected)} data={d} />
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

interface ModuleProps {
  data: Module;
  selected: boolean;
  onUpdate?: () => void;
  onRemove?: () => void;
}

const Module = (props: ModuleProps) => (
  <div className="module">
    <div className="module-header">
      <div className="module-name">
        {props.data.name}
        {props.selected && props.onUpdate && (
          <span
            onClick={(e) => {
              props.onUpdate && props.onUpdate.call(this);
              e.stopPropagation();
            }}
            className="module-update">
            <ArrowUp />
          </span>
        )}
        {props.selected && props.onRemove && (
          <span
            onClick={(e) => {
              props.onRemove && props.onRemove.call(this);
              e.stopPropagation();
            }}
            className="module-remove">
            <Trash2 />
          </span>
        )}
      </div>
      {props.data.repo && <img className="module-image" src={props.data.repo.owner.avatar_url} loading="lazy" />}
    </div>
    <div className="module-stats">
      {Boolean(props.data.releases?.length) && (
        <div className="module-version">
          <GitCommit className="module-icon" />
          {(props.data.releases[0] && props.data.releases[0].tag_name) || "unknown"}
        </div>
      )}
      {Boolean(props.data.releases?.length) && props.data.releases[0].published_at && (
        <div className="module-age">
          <HistoryIcon className="module-icon" />
          {(props.data.releases[0] && since(props.data.releases[0].published_at)) || "unknown"}
        </div>
      )}
      {props.data.repo && (
        <div className="module-stars">
          <Star className="module-icon" />
          {props.data.repo.stargazers_count.toLocaleString()}
        </div>
      )}
      {props.data.repo && (
        <div className="module-repo">
          <Github className="module-icon" />
          {props.data.repo.full_name}
        </div>
      )}
    </div>
    {props.data.repo && <div className="module-description">{props.data.repo.description}</div>}
  </div>
);

export function since(date: string) {
  let diff = (Date.now() - new Date(date).getTime()) / 1000;
  return roundedDurationSec(diff);
}

export function popularity(a: any, b: any) {
  let aScore = a.repo.stargazers_count;
  let bScore = b.repo.stargazers_count;
  if (a.name.includes("rules_")) aScore = aScore * 100;
  if (b.name.includes("rules_")) bScore = bScore * 100;
  return bScore - aScore;
}
