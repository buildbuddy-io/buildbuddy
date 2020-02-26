import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
  limitResults: boolean,
}

interface State {
  limit: number
}

const defaultPageSize = 10;

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state = {
    limit: defaultPageSize
  }

  handleArtifactClicked(outputUri: string) {
    window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", outputUri);
  }

  handleMoreArtifactClicked() {
    this.setState({ ...this.state, limit: this.state.limit ? undefined : defaultPageSize })
  }

  render() {
    return <div className="card artifacts">
      <img className="icon" src="/image/arrow-down-circle.svg" />
      <div className="content">
        <div className="title">Artifacts</div>
        <div className="details">
          {this.props.model.succeeded
            .filter(completed => completed.completed.importantOutput.length)
            .slice(0, this.props.limitResults && this.state.limit || undefined)
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
        {this.props.limitResults && this.props.model.succeeded.flatMap(
          completed => completed.completed.importantOutput).length > defaultPageSize && !!this.state.limit &&
          <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See more artifacts</div>}
        {this.props.limitResults && this.props.model.succeeded.flatMap(
          completed => completed.completed.importantOutput).length > defaultPageSize && !this.state.limit &&
          <div className="more" onClick={this.handleMoreArtifactClicked.bind(this)}>See less artifacts</div>}
      </div>
    </div>
  }
}
