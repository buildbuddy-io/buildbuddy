import { ArrowDownCircle, FileCode } from "lucide-react";
import React from "react";

import { zip } from "../../proto/zip_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService from "../service/rpc_service";

interface Props {
  name: string;
  files: build_event_stream.File[];
  invocationId: string;
}

interface State {
  loading: boolean;
  manifest: zip.IManifest;
}

export default class TargetArtifactsCardComponent extends React.Component<Props, State> {
  static readonly ZIPPED_OUTPUTS_FILE: string = "test.outputs__outputs.zip";

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
    if (!capabilities.config.testOutputManifestsEnabled) {
      return;
    }
    let testOutputsUri = this.props.files.find(
      (file: build_event_stream.File) => true && file.name === TargetArtifactsCardComponent.ZIPPED_OUTPUTS_FILE
    )?.uri;

    if (!testOutputsUri || !testOutputsUri.startsWith("bytestream://")) {
      return;
    }

    this.setState({ ...this.state, loading: true });
    const request = new zip.GetZipManifestRequest();
    request.uri = testOutputsUri;
    rpcService.service
      .getZipManifest(request)
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

  encodeManifestEntry(entry: zip.IManifestEntry): string {
    return btoa(
      zip.ManifestEntry.encode(entry)
        .finish()
        .reduce((str, b) => str + String.fromCharCode(b), "")
    );
  }

  makeArtifactUri(baseUri: string, entry: zip.IManifestEntry): string {
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

  handleZipArtifactClicked(outputUri: string, outputFilename: string, entry: zip.IManifestEntry, event: MouseEvent) {
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
                {output.name === TargetArtifactsCardComponent.ZIPPED_OUTPUTS_FILE &&
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
