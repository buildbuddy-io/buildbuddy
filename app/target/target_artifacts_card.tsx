import React from "react";

import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  files: build_event_stream.File[];
  invocationId: string;
}

export default class TargetArtifactsCardComponent extends React.Component {
  props: Props;

  handleArtifactClicked(outputUri: string, outputFilename: string) {
    if (!outputUri) return;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamFile(outputFilename, outputUri, this.props.invocationId);
    }
  }

  render() {
    return (
      <div className="card artifacts">
        <img className="icon" src="/image/arrow-down-circle.svg" />
        <div className="content">
          <div className="title">Artifacts</div>
          <div className="details">
            {this.props.files.map((file) => (
              <div className="artifact-name" onClick={this.handleArtifactClicked.bind(this, file.uri, file.name)}>
                {file.name}
              </div>
            ))}
          </div>
          {this.props.files.length == 0 && <span>No artifacts</span>}
        </div>
      </div>
    );
  }
}
