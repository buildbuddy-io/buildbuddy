import React from "react";
import { Profile, readProfileFile } from "./trace_events";

interface Props {
  children: (state: State) => React.ReactNode;
  className?: string;
  onProfileLoaded: (profile: Profile, fileName: string) => void;
  onProfileLoadError?: (error: unknown) => void;
}

interface State {
  dragActive: boolean;
  loadingMessage: string;
}

export default class TimingProfileDropTarget extends React.Component<Props, State> {
  private dragDepth = 0;
  private fileInputRef = React.createRef<HTMLInputElement>();

  state: State = {
    dragActive: false,
    loadingMessage: "",
  };

  openFilePicker() {
    this.fileInputRef.current?.click();
  }

  private async handleFile(file: File) {
    this.setState({ dragActive: false, loadingMessage: `Loading ${file.name}...` });

    try {
      const profile = await readProfileFile(file);
      this.props.onProfileLoaded(profile, file.name);
      this.setState({ loadingMessage: "" });
    } catch (e) {
      this.props.onProfileLoadError?.(e);
      this.setState({ loadingMessage: "" });
    }
  }

  private dragContainsFiles(e: React.DragEvent<HTMLElement>) {
    return Array.from(e.dataTransfer?.types || []).includes("Files");
  }

  private onDragEnter(e: React.DragEvent<HTMLDivElement>) {
    if (!this.dragContainsFiles(e)) return;
    e.preventDefault();
    this.dragDepth += 1;
    if (!this.state.dragActive) {
      this.setState({ dragActive: true });
    }
  }

  private onDragOver(e: React.DragEvent<HTMLDivElement>) {
    if (!this.dragContainsFiles(e)) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }

  private onDragLeave(e: React.DragEvent<HTMLDivElement>) {
    if (!this.dragContainsFiles(e)) return;
    e.preventDefault();
    this.dragDepth = Math.max(0, this.dragDepth - 1);
    if (this.dragDepth === 0) {
      this.setState({ dragActive: false });
    }
  }

  private onDrop(e: React.DragEvent<HTMLDivElement>) {
    if (!this.dragContainsFiles(e)) return;
    e.preventDefault();
    this.dragDepth = 0;
    this.setState({ dragActive: false });

    const file = e.dataTransfer.files.item(0);
    if (!file) return;
    this.handleFile(file);
  }

  private onFileInputChanged(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.item(0);
    if (file) {
      this.handleFile(file);
    }
    e.target.value = "";
  }

  render() {
    return (
      <div
        className={`${this.props.className || ""} ${this.state.dragActive ? "drag-active" : ""} ${
          this.state.loadingMessage ? "profile-loading" : ""
        }`.trim()}
        onDragEnter={this.onDragEnter.bind(this)}
        onDragOver={this.onDragOver.bind(this)}
        onDragLeave={this.onDragLeave.bind(this)}
        onDrop={this.onDrop.bind(this)}>
        {this.props.children(this.state)}
        <input
          ref={this.fileInputRef}
          style={{ display: "none" }}
          type="file"
          onChange={this.onFileInputChanged.bind(this)}
        />
      </div>
    );
  }
}
