import React from 'react';

import InvocationModel from './invocation_model'
import TargetsCardComponent from './invocation_targets_card'

interface Props {
  model: InvocationModel,
  pageSize: number,
  filter: string,
}

export default class TargetsComponent extends React.Component {
  props: Props;

  render() {
    return <div>
      {!!this.props.model.failed.length &&
        <TargetsCardComponent
          buildEvents={this.props.model.failed}
          iconPath="/image/x-circle.svg"
          presentVerb="failing"
          pastVerb="failed"
          model={this.props.model}
          filter={this.props.filter}
          pageSize={this.props.pageSize}
        />}

      {!!this.props.model.broken.length &&
        <TargetsCardComponent
          buildEvents={this.props.model.broken}
          iconPath="/image/x-circle.svg"
          presentVerb="broken"
          pastVerb="broken"
          model={this.props.model}
          filter={this.props.filter}
          pageSize={this.props.pageSize}
        />}

      {!!this.props.model.flaky.length &&
        <TargetsCardComponent
          buildEvents={this.props.model.flaky}
          iconPath="/image/x-circle.svg"
          presentVerb="flaky"
          pastVerb="flaky"
          model={this.props.model}
          filter={this.props.filter}
          pageSize={this.props.pageSize}
        />}

      {!!this.props.model.succeeded.length &&
        <TargetsCardComponent
          buildEvents={this.props.model.succeeded}
          iconPath="/image/check-circle.svg"
          presentVerb="passing"
          pastVerb="passed"
          model={this.props.model}
          filter={this.props.filter}
          pageSize={this.props.pageSize}
        />}
    </div>
  }
}
