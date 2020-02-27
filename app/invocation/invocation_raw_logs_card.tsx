import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
}

export default class RawLogsCardComponent extends React.Component {
  props: Props;

  render() {
    return <div className="card">
      <img className="icon" src="/image/log-circle.svg" />
      <div className="content">
        <div className="title">Raw logs</div>
        <div className="disclaimer">Raw logs can be large and may affect browser performance.</div>
        <div className="details code">
          {this.props.model.invocations.flatMap(invocation => JSON.stringify(invocation.toJSON(), null, 4))}
        </div>
      </div>
    </div>
  }
}
