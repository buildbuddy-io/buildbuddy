import React from 'react';
import InvocationModel from './invocation_model'
import rpcService from '../service/rpc_service';

interface Props {
  model: InvocationModel,
  pageSize: number,
  filter: string
}

interface State {
  numPages: number
}

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state: State = {
    numPages: 1
  }

  handleArtifactClicked(outputUri: string, outputFilename: string, invocationId: string) {
    if (!outputUri) return;

    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else if (outputUri.startsWith("bytestream://")) {
      rpcService.downloadBytestreamFile(outputFilename, outputUri, this.props.model.getId())
    }
  }

  handleMoreArtifactClicked() {
    this.setState({ ...this.state, numPages: this.state.numPages + 1 })
  }

  render() {
    let targets = this.props.model.succeeded
      .filter(completed => completed.completed.importantOutput.length)
      .filter(target => !this.props.filter ||
        target.id.targetCompleted.label.toLowerCase().includes(this.props.filter.toLowerCase()) ||
        target.completed.importantOutput.some(output => output.name.toLowerCase().includes(this.props.filter.toLowerCase())));

    return <div className="card artifacts">
      <img className="icon" src="/image/arrow-down-circle.svg" />
      <div className="content">
        <div className="title">Artifacts</div>
        <div className="details">
          {
            targets.slice(0, this.props.pageSize && (this.state.numPages * this.props.pageSize) || undefined)
              .map(completed => <div>
                <div className="artifact-section-title">{completed.id.targetCompleted.label}</div>
                {completed.completed.importantOutput
                  .filter(output => !this.props.filter ||
                    completed.id.targetCompleted.label.toLowerCase().includes(this.props.filter.toLowerCase()) ||
                    output.name.toLowerCase().includes(this.props.filter.toLowerCase()))
                  .map(output =>
                    <div className="artifact-name" onClick={this.handleArtifactClicked.bind(this, output.uri, output.name)}>
                      {output.name}
                    </div>
                  )}
              </div>
              )}
          {targets.flatMap(completed => completed.completed.importantOutput).length == 0 &&
            <span>{this.props.filter ? 'No matching artifacts' : 'No artifacts'}</span>}
        </div>
        {this.props.pageSize && targets.length > (this.state.numPages * this.props.pageSize) && !!this.state.numPages &&
          <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See more artifacts</div>}
      </div>
    </div>
  }
}
