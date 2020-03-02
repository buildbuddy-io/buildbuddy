import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
  pageSize: number,
}

interface State {
  numPages: number
}

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state: State = {
    numPages: 1
  }

  handleArtifactClicked(outputUri: string) {
    if (!outputUri) return;
    if (outputUri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
    } else {
      window.open(outputUri, '_blank');
    }
  }

  handleMoreArtifactClicked() {
    this.setState({ ...this.state, numPages: this.state.numPages + 1 })
  }

  render() {
    return <div className="card artifacts">
      <img className="icon" src="/image/arrow-down-circle.svg" />
      <div className="content">
        <div className="title">Artifacts</div>
        <div className="details">
          {this.props.model.succeeded
            .filter(completed => completed.completed.importantOutput.length)
            .slice(0, this.props.pageSize && (this.state.numPages * this.props.pageSize) || undefined)
            .map(completed => <div>
              <div className="artifact-section-title">{completed.id.targetCompleted.label}</div>
              {completed.completed.importantOutput.map(output =>
                <div className="artifact-name" onClick={this.handleArtifactClicked.bind(this, output.uri)}>
                  {output.name}
                </div>
              )}
            </div>
            )}
          {this.props.model.succeeded.flatMap(completed => completed.completed.importantOutput).length == 0 &&
            <span>No artifacts</span>}
        </div>
        {this.props.pageSize && this.props.model.succeeded.length > (this.state.numPages * this.props.pageSize) && !!this.state.numPages &&
          <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See more artifacts</div>}
      </div>
    </div>
  }
}
