import { ArrowDownCircle, FileCode } from "lucide-react";
import React from "react";

import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  files: build_event_stream.File[];
  invocationId: string;
}

export default class TargetArtifactsCardComponent extends React.Component<Props> {
  handleArtifactClicked(outputUri: string, outputFilename: string, event: MouseEvent) {
    event.preventDefault();
    if (!outputUri) return false;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamFile(outputFilename, outputUri, this.props.invocationId);
    }
    return false;
  }

  render() {
    return (
      <div className="card artifacts">
        <ArrowDownCircle className="icon brown" />
        <div className="content">
          <div className="title">Artifacts</div>
          <div className="details">
            {this.props.files.map((output) => (
              <div className="artifact-line">
                <a
                  href={rpcService.getBytestreamUrl(output.uri, this.props.invocationId, {
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
                    )}&invocation_id=${this.props.invocationId}&filename=${output.name}`}>
                    <FileCode /> View
                  </a>
                )}
              </div>
            ))}
          </div>
          {this.props.files.length == 0 && <span>No artifacts</span>}
        </div>
      </div>
    );
  }
}
