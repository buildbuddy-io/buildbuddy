import React from 'react';
import moment from 'moment';

import rpcService from '../service/rpc_service'
import authService, { User } from '../auth/auth_service';
import capabilities from '../capabilities/capabilities';

import InvocationModel from './invocation_model'

import InvocationLoadingComponent from './invocation_loading'
import InvocationInProgressComponent from './invocation_in_progress'
import InvocationNotFoundComponent from './invocation_not_found'

import InvocationOverviewComponent from './invocation_overview'
import InvocationTabsComponent from './invocation_tabs';
import InvocationFilterComponent from './invocation_filter';
import BuildLogsCardComponent from './invocation_build_logs_card'
import ErrorCardComponent from './invocation_error_card';
import InvocationDetailsCardComponent from './invocation_details_card'
import ArtifactsCardComponent from './invocation_artifacts_card'
import RawLogsCardComponent from './invocation_raw_logs_card'
import TimingCardComponent from './invocation_timing_card'
import TargetsComponent from './invocation_targets'
import TargetComponent from '../target/target'

import DenseInvocationOverviewComponent from './dense/dense_invocation_overview'
import DenseInvocationTabsComponent from './dense/dense_invocation_tabs'

import { invocation } from '../../proto/invocation_ts_proto';

interface State {
  loading: boolean,
  inProgress: boolean,
  notFound: boolean,

  model: InvocationModel,
}

interface Props {
  user?: User,
  invocationId: string,
  hash: string,
  search: URLSearchParams,
  denseMode: boolean,
}

const largePageSize = 100;
const smallPageSize = 10;

export default class InvocationComponent extends React.Component {
  state: State = {
    loading: true,
    inProgress: false,
    notFound: false,

    model: new InvocationModel(),
  };

  props: Props;

  timeoutRef: number;

  componentWillMount() {
    document.title = `Invocation ${this.props.invocationId} | Buildbuddy`;
    // TODO(siggisim): Move moment configuration elsewhere
    moment.relativeTimeThreshold('ss', 0);

    this.fetchInvocation();
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  fetchInvocation() {
    let request = new invocation.GetInvocationRequest();
    request.requestContext = authService.getRequestContext();
    request.lookup = new invocation.InvocationLookup();
    request.lookup.invocationId = this.props.invocationId;
    rpcService.service.getInvocation(request).then((response) => {
      console.log(response);
      var showInProgressScreen = false;
      if (response.invocation.length && response.invocation[0].invocationStatus ==
        invocation.Invocation.InvocationStatus.PARTIAL_INVOCATION_STATUS) {
        showInProgressScreen = response.invocation[0].event.length == 0;
        this.fetchUpdatedProgress();
      }

      this.setState({
        inProgress: showInProgressScreen,
        model: InvocationModel.modelFromInvocations(response.invocation as invocation.Invocation[]),
        loading: false
      });
      document.title = `${this.state.model.getUser(true)} ${this.state.model.getCommand()} ${this.state.model.getPattern()} | BuildBuddy`;
    }).catch((error: any) => {
      console.error(error);
      this.setState({
        notFound: true,
        loading: false
      });
    });
  }

  fetchUpdatedProgress() {
    clearTimeout(this.timeoutRef);
    // Refetch invocation data in 3 seconds to update status.
    this.timeoutRef = setTimeout(() => {
      this.fetchInvocation();
    }, 3000);
  }

  render() {
    if (this.state.loading) {
      return <InvocationLoadingComponent invocationId={this.props.invocationId} />;
    }

    if (this.state.notFound) {
      return <InvocationNotFoundComponent invocationId={this.props.invocationId} authorized={!capabilities.auth || !!this.props.user} />;
    }

    if (this.state.inProgress) {
      return <InvocationInProgressComponent invocationId={this.props.invocationId} />;
    }

    let targetLabel = this.props.search.get("target");
    if (targetLabel) {
      return <TargetComponent invocationId={this.props.invocationId}
        hash={this.props.hash}
        configuredEvent={this.state.model.configuredMap.get(targetLabel)}
        completedEvent={this.state.model.completedMap.get(targetLabel)}
        testResultEvents={this.state.model.testResultMap.get(targetLabel)}
        testSummaryEvent={this.state.model.testSummaryMap.get(targetLabel)}
        user={this.props.user}
        targetLabel={targetLabel} />;
    }

    var showAll = !this.props.hash && !this.props.denseMode;

    return (
      <div>
        <div className={`shelf ${this.state.model.getStatusClass()}`}>
          {this.props.denseMode ?
            <DenseInvocationOverviewComponent invocationId={this.props.invocationId} model={this.state.model} /> :
            <InvocationOverviewComponent invocationId={this.props.invocationId} model={this.state.model} user={this.props.user} />}

        </div>
        <div className="container">
          {this.props.denseMode ?
            <DenseInvocationTabsComponent hash={this.props.hash} /> :
            <InvocationTabsComponent hash={this.props.hash} />}

          {((!this.props.hash && this.props.denseMode) || this.props.hash == "#targets" || this.props.hash == "#artifacts") &&
            <InvocationFilterComponent hash={this.props.hash} search={this.props.search} />}

          {(showAll || this.props.hash == "#log") &&
            this.state.model.aborted?.aborted.description &&
            <ErrorCardComponent model={this.state.model} />}

          {(!this.props.hash || this.props.hash == "#targets") &&
            <TargetsComponent
              model={this.state.model}
              mode="failing"
              filter={this.props.search.get("targetFilter")}
              pageSize={showAll ? smallPageSize : largePageSize} />}

          {(showAll || this.props.hash == "#log") &&
            <BuildLogsCardComponent model={this.state.model} expanded={this.props.hash == "#log"} />}

          {(!this.props.hash || this.props.hash == "#targets") &&
            <TargetsComponent
              model={this.state.model}
              mode="passing"
              filter={this.props.search.get("targetFilter")}
              pageSize={showAll ? smallPageSize : largePageSize} />}

          {(showAll || this.props.hash == "#details") &&
            <InvocationDetailsCardComponent model={this.state.model} limitResults={!this.props.hash} />}

          {(showAll || this.props.hash == "#artifacts") &&
            <ArtifactsCardComponent
              model={this.state.model}
              filter={this.props.search.get("artifactFilter")}
              pageSize={this.props.hash ? largePageSize : smallPageSize} />}

          {(this.props.hash == "#timing") && <TimingCardComponent model={this.state.model} />}
          {(this.props.hash == "#raw") && <RawLogsCardComponent model={this.state.model} pageSize={largePageSize} />}
        </div>
      </div>
    );
  }
}
