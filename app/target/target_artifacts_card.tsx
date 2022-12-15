import { ArrowDownCircle, FileCode } from "lucide-react";
import React from "react";

import { archive } from "../../proto/archive_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import rpcService from "../service/rpc_service";

interface Props {
  name: string;
  files: build_event_stream.File[];
  invocationId: string;
}

interface State {
  loading: boolean;
  manifest: archive.IArchiveManifest;
}

export default class TargetArtifactsCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    manifest: null,
  };

  componentDidMount() {
    this.maybeFetchOutputManifest();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.files !== prevProps.files) {
      this.maybeFetchOutputManifest();
    }
  }

  maybeFetchOutputManifest() {
    let testLogUrl = this.props.files.find(
      (file: build_event_stream.File) => true && file.name === "test.outputs__outputs.zip"
    )?.uri;

    if (!testLogUrl) {
      return;
    }

    if (!testLogUrl.startsWith("bytestream://")) {
      this.setState({ ...this.state, cacheEnabled: false });
      return;
    }

    this.setState({ ...this.state, loading: true });
    const request = new archive.GetArchiveManifestRequest();
    request.uri = testLogUrl;
    rpcService.service
      .getArchiveManifest(request)
      .then((response) => {
        this.setState({ ...this.state, manifest: response.manifest, loading: false });
      })
      .catch(() => {
        this.setState({
          ...this.state,
          loading: false,
          manifest: null,
        });
      });
  }

  encodeManifestEntry(entry: archive.IManifestEntry): string {
    return btoa(
      archive.ManifestEntry.encode(entry)
        .finish()
        .reduce((str, b) => str + String.fromCharCode(b), "")
    );
  }

  makeArtifactUri(baseUri: string, entry: archive.IManifestEntry): string {
    return rpcService.getBytestreamUrl(baseUri, this.props.invocationId, {
      filename: entry.name,
      zip: this.encodeManifestEntry(entry),
    });
  }

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

  handleZipArtifactClicked(
    outputUri: string,
    outputFilename: string,
    entry: archive.IManifestEntry,
    event: MouseEvent
  ) {
    event.preventDefault();
    if (!outputUri) return false;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamZipFile(
        outputFilename,
        outputUri,
        this.encodeManifestEntry(entry),
        this.props.invocationId
      );
    }
    return false;
  }

  render() {
    return (
      <div className="card artifacts">
        <ArrowDownCircle className="icon brown" />
        <div className="content">
          <div className="title">Artifacts: {this.props.name}</div>
          <div className="details">
            {this.props.files.map((output) => (
              <>
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
                {output.name === "test.outputs__outputs.zip" &&
                  this.state.manifest &&
                  this.state.manifest.entry?.map((entry) => (
                    <div className="artifact-line sub-item">
                      <a
                        href={this.makeArtifactUri(output.uri, entry)}
                        className="artifact-name"
                        onClick={this.handleZipArtifactClicked.bind(this, output.uri, entry.name, entry)}>
                        {entry.name}
                      </a>
                    </div>
                  ))}
              </>
            ))}
          </div>
          {this.props.files.length == 0 && <span>No artifacts</span>}
        </div>
      </div>
    );
  }
}
