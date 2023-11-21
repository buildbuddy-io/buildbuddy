import React from "react";
import InvocationModel from "./invocation_model";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { target } from "../../proto/target_ts_proto";
import { ArrowDownCircle, FileCode } from "lucide-react";
import TargetGroupCard from "./invocation_target_group_card";
import format from "../format/format";
import DigestComponent from "../components/digest/digest";

interface Props {
  model: InvocationModel;
  pageSize: number;
  filter: string;
}

interface State {
  numPages: number;

  searchLoading: boolean;
  searchResponse?: target.GetTargetResponse;
}

export default class ArtifactsCardComponent extends React.Component<Props, State> {
  state: State = {
    numPages: 1,
    searchLoading: false,
  };

  componentDidMount() {
    if (this.props.filter) {
      this.search();
    }
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.filter !== prevProps.filter) {
      this.search();
    }
  }

  private searchRPC?: CancelablePromise;

  private search() {
    this.searchRPC?.cancel();
    this.setState({ searchLoading: true });
    if (!this.props.filter) {
      this.setState({ searchLoading: false, searchResponse: undefined });
      return;
    }
    this.searchRPC = rpcService.service
      .getTarget({
        status: 0, // only fetch the top-level artifact listing
        invocationId: this.props.model.getInvocationId(),
        filter: this.props.filter,
      })
      .then((response) => this.setState({ searchResponse: response }))
      .finally(() => this.setState({ searchLoading: false }));
  }

  handleArtifactClicked(outputUri: string, outputFilename: string, event: React.MouseEvent<HTMLAnchorElement>) {
    event.preventDefault();
    if (!outputUri) return false;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamFile(outputFilename, outputUri, this.props.model.getInvocationId());
    }
  }

  handleMoreArtifactsClicked() {
    this.setState({ numPages: this.state.numPages + 1 });
  }

  render() {
    if (this.props.model.invocation.targetGroups.length) {
      const artifactListingGroup = this.props.model.invocation.targetGroups.find((group) => group.status === 0);
      if (!artifactListingGroup) return null;

      if (this.state.searchLoading) {
        return <div className="loading" />;
      }

      const group = this.state.searchResponse?.targetGroups[0] ?? artifactListingGroup;
      return (
        <TargetGroupCard invocationId={this.props.model.getInvocationId()} group={group} filter={this.props.filter} />
      );
    }

    type Target = {
      label: string;
      outputs: build_event_stream.File[];
      // The number of outputs hidden due to paging limits.
      hiddenOutputCount: number;
    };
    const filteredTargets = this.props.model.succeeded
      .map((event) => ({
        label: event.id?.targetCompleted?.label ?? "",
        outputs: this.props.model
          .getFiles(event)
          .filter(
            (output) =>
              !this.props.filter ||
              event.id?.targetCompleted?.label.toLowerCase().includes(this.props.filter.toLowerCase()) ||
              output.name.toLowerCase().includes(this.props.filter.toLowerCase())
          ),
      }))
      .filter((target) => target.label && target.outputs.length);

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
                        href={rpcService.getBytestreamUrl(output.uri, this.props.model.getInvocationId(), {
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
                          )}&invocation_id=${this.props.model.getInvocationId()}&filename=${output.name}`}>
                          <FileCode /> View
                        </a>
                      )}
                      <DigestComponent digest={{ hash: output.digest, sizeBytes: output.length }} />
                    </div>
                  ))}
                  {target.hiddenOutputCount > 0 && (
                    <div className="artifact-hidden-count">
                      {format.formatWithCommas(target.hiddenOutputCount)} more{" "}
                      {target.hiddenOutputCount === 1 ? "artifact" : "artifacts"} for this target
                    </div>
                  )}
                </div>
              </div>
            ))}
            {hiddenTargetCount > 0 && (
              <div className="artifact-hidden-count">
                {format.formatWithCommas(hiddenTargetCount)} more {hiddenTargetCount === 1 ? "target" : "targets"} with
                artifacts
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
