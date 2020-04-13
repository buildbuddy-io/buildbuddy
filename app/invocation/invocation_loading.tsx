import React from 'react';

interface Props {
  invocationId: string,
}

export default class InvocationLoadingComponent extends React.Component {
  props: Props;

  render() {
    return <div className="state-page">
      <div className="shelf">
        <div className="container">
          <div className="breadcrumbs">Invocation {this.props.invocationId}</div>
          <div className="titles">
            <div className="title">Loading...</div>
          </div>
          <div className="details">Loading...</div>
        </div>
      </div>
    </div>
  }
}
