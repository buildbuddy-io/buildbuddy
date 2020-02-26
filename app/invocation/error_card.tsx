import React from 'react';
import InvocationModel from './invocation_model'

interface Props {
  model: InvocationModel,
}


export default class ErrorCardComponent extends React.Component {
  props: Props;

  render() {
    return <div className="card">
      <img className="icon" src="/image/alert-circle.svg" />
      <div className="content">
        <div className="title">Error</div>
        <div className="details">
          {this.props.model.aborted.aborted.description}
        </div>
      </div>
    </div>
  }
}
