import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
  limitResults: boolean,
}

interface State {
  limit: number
}

const defaultPageSize = 1;

export default class ArtifactsCardComponent extends React.Component {
  props: Props;

  state: State = {
    limit: defaultPageSize
  }

  handleMoreInvocationClicked() {
    this.setState({ ...this.state, limit: this.state.limit ? undefined : defaultPageSize })
  }

  render() {
    return <div className="card">
      <img className="icon" src="/image/info.svg" />
      <div className="content">
        <div className="title">Invocation details</div>
        <div className="details">
          {this.props.model.structuredCommandLine
            .filter(commandLine => commandLine.commandLineLabel && commandLine.commandLineLabel.length)
            .sort((a, b) => {
              return a.commandLineLabel < b.commandLineLabel ? -1 : 1;
            })
            .slice(0, this.props.limitResults && this.state.limit ? this.state.limit : undefined)
            .map(commandLine =>
              <div className="invocation-command-line">
                <div className="invocation-command-line-title">{commandLine.commandLineLabel}</div>
                {commandLine.sections
                  .flatMap(section =>
                    <div className="invocation-section">
                      <div className="invocation-section-title">
                        {section.sectionLabel}
                      </div>
                      <div>
                        {section.chunkList?.chunk.map(chunk => <div className="invocation-chunk">{chunk}</div>) || []}
                        {section.optionList?.option.map(option => <div>
                          <span className="invocation-option-dash">--</span>
                          <span className="invocation-option-name">{option.optionName}</span>
                          <span className="invocation-option-equal">=</span>
                          <span className="invocation-option-value">{option.optionValue}</span>
                        </div>) || []}
                      </div>
                    </div>
                  )}
              </div>)}
        </div>
        {this.props.limitResults && !!this.state.limit &&
          <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See more invocation details</div>}
        {this.props.limitResults && !this.state.limit &&
          <div className="more" onClick={this.handleMoreInvocationClicked.bind(this)}>See less invocation details</div>}
      </div>
    </div>
  }
}
