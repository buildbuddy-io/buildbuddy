import React from "react";
import InvocationModel from "./invocation_model";
import rpcService from "../service/rpc_service";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { ArrowDownCircle, FileCode } from "lucide-react";

interface Props {
  model: InvocationModel;
  pageSize: number;
  filter: string;
}

interface State {
  numPages: number;
}

export default class ArtifactsCardComponent extends React.Component<Props, State> {
  state: State = {
    numPages: 1,
  };

  handleArtifactClicked(outputUri: string, outputFilename: string, event: MouseEvent) {
    event.preventDefault();
    if (!outputUri) return false;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamFile(outputFilename, outputUri, this.props.model.getId());
    }
  }

  handleMoreArtifactsClicked() {
    this.setState({ ...this.state, numPages: this.state.numPages + 1 });
  }

  render() {
    type Target = {
      label: string;
      outputs: build_event_stream.IFile[];
      // The number of outputs hidden due to paging limits.
      hiddenOutputCount?: number;
    };
    const filteredTargets = this.props.model.succeeded
      .map((event) => ({
        label: event.id.targetCompleted.label,
        outputs: this.props.model
          .getFiles(event)
          .filter(
            (output) =>
              !this.props.filter ||
              event.id.targetCompleted.label.toLowerCase().includes(this.props.filter.toLowerCase()) ||
              output.name.toLowerCase().includes(this.props.filter.toLowerCase())
          ),
      }))
      .filter((target) => target.outputs.length);

    const visibleTargets: Target[] = [];
    let visibleOutputCount = 0;
    const visibleOutputLimit =
      (this.props.pageSize && this.state.numPages * this.props.pageSize) || Number.MAX_SAFE_INTEGER;
    const totalOutputCount = filteredTargets.map((target) => target.outputs.length).reduce((acc, val) => acc + val, 0);

    for (const target of filteredTargets) {
      const visibleOutputs = target.outputs.slice(0, visibleOutputLimit - visibleOutputCount);
      if (!visibleOutputs.length) break;

      visibleOutputCount += visibleOutputs.length;
      visibleTargets.push({
        ...target,
        outputs: visibleOutputs,
        hiddenOutputCount: target.outputs.length - visibleOutputs.length,
      });
    }
    const hiddenTargetCount = filteredTargets.length - visibleTargets.length;

    return (
      <div className="card artifacts">
        <ArrowDownCircle className="icon brown" />
        <div className="content">
          <div className="title">Artifacts</div>
          <div className="details">
            {visibleTargets.map((target) => (
              <div>
                <div className="artifact-section-title">{target.label}</div>
                <div className="artifact-list">
                  {target.outputs.map((output) => (
                    <div className="artifact-line">
                      <a
                        href={rpcService.getBytestreamUrl(output.uri, this.props.model.getId(), {
                          filename: output.name,
                        })}
                        className="artifact-name"
                        onClick={this.handleArtifactClicked.bind(this, output.uri, output.name)}>
                        {output.name}
                      </a>
                      {output.uri?.startsWith("bytestream://") && (
                        <a
                          className="artifact-view"
                          href={`/code/buildbuddy-io/buildbuddy/?bytestream_url=${encodeURIComponent(
                            output.uri
                          )}&invocation_id=${this.props.model.getId()}&filename=${output.name}`}>
                          <FileCode /> View
                        </a>
                      )}
                    </div>
                  ))}
                  {target.hiddenOutputCount > 0 && (
                    <div className="artifact-hidden-count">
                      {target.hiddenOutputCount} more {target.hiddenOutputCount === 1 ? "artifact" : "artifacts"} for
                      this target
                    </div>
                  )}
                </div>
              </div>
            ))}
            {hiddenTargetCount > 0 && (
              <div className="artifact-hidden-count">
                {hiddenTargetCount} more {hiddenTargetCount === 1 ? "target" : "targets"} with artifacts
              </div>
            )}
            {totalOutputCount === 0 && <span>{this.props.filter ? "No matching artifacts" : "No artifacts"}</span>}
          </div>
          {this.props.pageSize && visibleOutputCount < totalOutputCount && (
            <div className="more" onClick={this.handleMoreArtifactsClicked.bind(this)}>
              See more artifacts
            </div>
          )}
        </div>
      </div>
    );
  }
}
