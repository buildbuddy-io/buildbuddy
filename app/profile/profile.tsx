import { Upload } from "lucide-react";
import React from "react";
import alertService from "../alert/alert_service";
import { OutlinedButton } from "../components/button/button";
import InvocationBreakdownCardComponent from "../invocation/invocation_breakdown_card";
import TimingProfileDropTarget from "../trace/timing_profile_drop_target";
import { Profile } from "../trace/trace_events";
import TraceViewer from "../trace/trace_viewer";

interface Props {
  dark: boolean;
}

interface State {
  profile: Profile | null;
  durationByNameMap: Map<string, number>;
  durationByCategoryMap: Map<string, number>;
  error: string;
  fileName: string;
  viewerKey: number;
}

export default class TimingProfilePageComponent extends React.Component<Props, State> {
  private dropTargetRef = React.createRef<TimingProfileDropTarget>();

  state: State = {
    profile: null,
    durationByNameMap: new Map<string, number>(),
    durationByCategoryMap: new Map<string, number>(),
    error: "",
    fileName: "",
    viewerKey: 0,
  };

  private buildDurationMaps(profile: Profile) {
    const durationByNameMap = new Map<string, number>();
    const durationByCategoryMap = new Map<string, number>();

    for (const event of profile.traceEvents || []) {
      if (!event.dur) continue;
      durationByNameMap.set(event.name, (durationByNameMap.get(event.name) || 0) + event.dur);
      durationByCategoryMap.set(event.cat, (durationByCategoryMap.get(event.cat) || 0) + event.dur);
    }

    return { durationByNameMap, durationByCategoryMap };
  }

  private updateProfile(profile: Profile, fileName: string) {
    this.setState({
      profile,
      ...this.buildDurationMaps(profile),
      fileName,
      error: "",
      viewerKey: this.state.viewerKey + 1,
    });
  }

  private handleProfileLoadError(e: unknown) {
    console.error(e);
    alertService.error("Failed to parse timing profile");
    this.setState({
      error: "Failed to parse timing profile. Make sure the file is a Bazel JSON trace profile or .gz archive.",
    });
  }

  private openFilePicker() {
    this.dropTargetRef.current?.openFilePicker();
  }

  private renderDropOverlay(label: string) {
    return (
      <div className="timing-profile-page-drop-overlay">
        <div className="timing-profile-page-drop-overlay-text">{label}</div>
      </div>
    );
  }

  private renderEmptyState() {
    return (
      <TimingProfileDropTarget
        ref={this.dropTargetRef}
        className="timing-profile-page-empty-state"
        onProfileLoaded={this.updateProfile.bind(this)}
        onProfileLoadError={this.handleProfileLoadError.bind(this)}>
        {({ dragActive, loadingMessage }) => (
          <>
            <Upload className="icon" />
            <div className="title">Drop a timing profile here</div>
            <div className="details">Supports JSON trace profiles and gzipped files such as `command.profile.gz`.</div>
            <OutlinedButton onClick={this.openFilePicker.bind(this)}>Choose file</OutlinedButton>
            {this.state.error && <div className="error-message">{this.state.error}</div>}
            {(dragActive || loadingMessage) &&
              this.renderDropOverlay(loadingMessage || "Drop a timing profile to render it")}
          </>
        )}
      </TimingProfileDropTarget>
    );
  }

  private renderViewer() {
    return (
      <>
        <div className="timing-profile-page-toolbar">
          <div>
            Showing <span className="inline-code">{this.state.fileName}</span>
          </div>
          <OutlinedButton onClick={this.openFilePicker.bind(this)}>Choose another file</OutlinedButton>
        </div>
        <TimingProfileDropTarget
          ref={this.dropTargetRef}
          className="timing-profile-page-viewer"
          onProfileLoaded={this.updateProfile.bind(this)}
          onProfileLoadError={this.handleProfileLoadError.bind(this)}>
          {({ dragActive, loadingMessage }) => (
            <>
              <TraceViewer key={this.state.viewerKey} profile={this.state.profile!} dark={this.props.dark} />
              {(dragActive || loadingMessage) &&
                this.renderDropOverlay(loadingMessage || "Drop a timing profile to replace it")}
            </>
          )}
        </TimingProfileDropTarget>
        <InvocationBreakdownCardComponent
          durationByNameMap={this.state.durationByNameMap}
          durationByCategoryMap={this.state.durationByCategoryMap}
        />
      </>
    );
  }

  render() {
    return (
      <div className="timing-profile-page">
        <div className="container timing-profile-page-content">
          {this.state.profile ? this.renderViewer() : this.renderEmptyState()}
        </div>
      </div>
    );
  }
}
