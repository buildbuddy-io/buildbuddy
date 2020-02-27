import React from 'react';

interface Props {
  hash: string,
}

export default class DenseInvocationTabsComponent extends React.Component {
  props: Props;

  render() {
    return <div className="tabs">
      <a href="#targets" className={`tab ${(this.props.hash == '#targets' || this.props.hash == '') && 'selected'}`}>
        TARGETS
      </a>
      <a href="#log" className={`tab ${this.props.hash == '#log' && 'selected'}`}>
        BUILD LOG
      </a>
      <a href="#details" className={`tab ${this.props.hash == '#details' && 'selected'}`}>
        INVOCATION DETAILS
      </a>
      <a href="#artifacts" className={`tab ${this.props.hash == '#artifacts' && 'selected'}`}>
        ARTIFACTS
      </a>
      <a href="#raw" className={`tab ${this.props.hash == '#raw' && 'selected'}`}>
        RAW LOG
      </a>
    </div>
  }
}
