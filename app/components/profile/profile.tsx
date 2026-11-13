import { Activity, Copy } from "lucide-react";
import React from "react";
import { FilledButton, OutlinedButton } from "../button/button";
import Select, { Option } from "../select/select";
import Spinner from "../spinner/spinner";
import alertService from "../../alert/alert_service";
import format from "../../format/format";
import { secondsToDuration } from "../../util/proto";
import { copyToClipboard } from "../../util/clipboard";
import { BuildBuddyError } from "../../util/errors";
import { google as google_duration } from "../../../proto/duration_ts_proto";
import { profile } from "../../../proto/profile_ts_proto";

const PROFILE_TYPES: { type: profile.ProfileType; label: string; description: string }[] = [
  {
    type: profile.ProfileType.CPU,
    label: "CPU",
    description: "Where the process spends CPU time.",
  },
  {
    type: profile.ProfileType.HEAP,
    label: "Heap",
    description: "In-use and allocated memory.",
  },
  {
    type: profile.ProfileType.GOROUTINE,
    label: "Goroutines",
    description: "Goroutine stacks, aggregated.",
  },
  {
    type: profile.ProfileType.GOROUTINE_DUMP,
    label: "Goroutine dump",
    description: "Full stack trace of every goroutine, as text.",
  },
];

const CPU_DURATIONS_SECONDS = [5, 10, 15, 20, 30];

const PPROF_COMMAND = "go tool pprof -http=: <file>";

interface Props {
  filePrefix: string;
  collect: (request: profile.GetProfileRequest) => Promise<profile.IProfile | null | undefined>;
}

interface State {
  type: profile.ProfileType;
  durationSeconds: number;
  collecting: boolean;
}

export default class ProfileCard extends React.Component<Props, State> {
  state: State = {
    type: profile.ProfileType.CPU,
    durationSeconds: 10,
    collecting: false,
  };

  async onCollect() {
    const type = this.state.type;
    const request = profile.GetProfileRequest.create({
      type,
      duration:
        type === profile.ProfileType.CPU
          ? google_duration.protobuf.Duration.create(secondsToDuration(this.state.durationSeconds))
          : undefined,
    });
    this.setState({ collecting: true });
    try {
      const response = await this.props.collect(request);
      if (response?.status?.code) {
        alertService.error(response.status.message || `Error collecting profile (error code ${response.status.code})`);
      } else if (!response?.data?.length) {
        alertService.error("The profile was empty.");
      } else {
        const name = fileName(this.props.filePrefix, type, new Date());
        download(name, response.data);
        alertService.success(`Downloaded ${name} (${format.bytes(response.data.length)})`);
      }
    } catch (e) {
      alertService.error(BuildBuddyError.parse(e).message);
    } finally {
      this.setState({ collecting: false });
    }
  }

  render() {
    const selected = PROFILE_TYPES.find((t) => t.type === this.state.type);
    const isCPU = this.state.type === profile.ProfileType.CPU;
    return (
      <div className="profile-card">
        <div className="profile-card-title">
          <Activity />
          Profiling
        </div>
        <div className="profile-card-controls">
          <Select
            value={this.state.type}
            disabled={this.state.collecting}
            onChange={(e) => this.setState({ type: Number(e.target.value) })}>
            {PROFILE_TYPES.map((t) => (
              <Option key={t.type} value={t.type}>
                {t.label}
              </Option>
            ))}
          </Select>
          {isCPU && (
            <Select
              value={this.state.durationSeconds}
              disabled={this.state.collecting}
              onChange={(e) => this.setState({ durationSeconds: Number(e.target.value) })}>
              {CPU_DURATIONS_SECONDS.map((s) => (
                <Option key={s} value={s}>
                  {s} seconds
                </Option>
              ))}
            </Select>
          )}
          <FilledButton disabled={this.state.collecting} onClick={() => this.onCollect()}>
            {this.state.collecting && <Spinner className="icon white" />}
            {this.state.collecting ? "Collecting..." : "Collect profile"}
          </FilledButton>
          <span className="profile-card-muted">{selected?.description}</span>
        </div>
        {!isText(this.state.type) && (
          <>
            <div className="profile-card-muted profile-card-hint">Open downloaded profiles with:</div>
            <div className="profile-card-command">
              <code>{PPROF_COMMAND}</code>
              <OutlinedButton className="profile-card-copy" onClick={() => copyCommand()}>
                <Copy className="icon" />
                Copy
              </OutlinedButton>
            </div>
          </>
        )}
      </div>
    );
  }
}

function copyCommand() {
  try {
    copyToClipboard(PPROF_COMMAND);
    alertService.success("Copied command to clipboard");
  } catch (e) {
    alertService.error(BuildBuddyError.parse(e).message);
  }
}

function isText(type: profile.ProfileType): boolean {
  return type === profile.ProfileType.GOROUTINE_DUMP;
}

function fileName(prefix: string, type: profile.ProfileType, time: Date): string {
  const typeName = profile.ProfileType[type].toLowerCase();
  const timestamp = time.toISOString().replace(/[:.]/g, "-");
  return `${prefix || "profile"}-${typeName}-${timestamp}.${isText(type) ? "txt" : "pb.gz"}`;
}

function download(name: string, data: Uint8Array) {
  const url = URL.createObjectURL(new Blob([data as BlobPart], { type: "application/octet-stream" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  // Revoking synchronously can cancel the download in some browsers.
  setTimeout(() => URL.revokeObjectURL(url));
}
