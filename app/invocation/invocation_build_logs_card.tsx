import React from 'react';
import InvocationModel from './invocation_model'
import { TerminalComponent } from '../terminal/terminal'

interface Props {
  model: InvocationModel,
  expanded: boolean
}

export default class BuildLogsCardComponent extends React.Component {
  props: Props;

  render() {
    return <div className={`card dark ${this.props.expanded ? "expanded" : ""}`}>
      <img className="icon" src="/image/log-circle-light.svg" />
      <div className="content">
        <div className="title">Build logs </div>
        <div className="details">
          <TerminalComponent value={this.props.model.consoleBuffer} />
        </div>
      </div>
    </div>
  }
}
