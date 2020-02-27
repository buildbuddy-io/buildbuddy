import React from 'react';

interface Props {
  invocationId: string,
}

export default class InvocationInProgressComponent extends React.Component {
  props: Props;

  render() {
    return <div className="state-page">
      <div className="shelf">
        <div className="container">
          <div className="invocation">Invocation {this.props.invocationId}</div>
          <div className="titles">
            <div className="title">Invocation in progress...</div>
          </div>
          <div className="details">This page will refresh in a few seconds.</div>
        </div>
      </div>
    </div>
  }
}
