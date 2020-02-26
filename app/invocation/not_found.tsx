import React from 'react';

interface Props {
  invocationId: string,
}

export default class InvocationNotFoundComponent extends React.Component {
  props: Props;

  render() {
    return <div>
      <div className="shelf">
        <div className="container">
          <div className="invocation">Invocation {this.props.invocationId}</div>
          <div className="titles">
            <div className="title">Invocation not found!</div>
          </div>
          <div className="details">Double check your invocation URL and try again.</div>
        </div>
      </div>
    </div>
  }
}
