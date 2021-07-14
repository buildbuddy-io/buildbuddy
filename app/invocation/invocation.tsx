import moment from "moment";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import faviconService from "../favicon/favicon";
import rpcService from "../service/rpc_service";
import TargetComponent from "../target/target";
import DenseInvocationOverviewComponent from "./dense/dense_invocation_overview";
import ArtifactsCardComponent from "./invocation_artifacts_card";
import BuildLogsCardComponent from "./invocation_build_logs_card";
import QueryGraphCardComponent from "./invocation_query_graph_card";
import CacheCardComponent from "./invocation_cache_card";
import FetchCardComponent from "./invocation_fetch_card";
import InvocationDetailsCardComponent from "./invocation_details_card";
import ErrorCardComponent from "./invocation_error_card";
import SuggestionCardComponent from "./invocation_suggestion_card";
import InvocationFilterComponent from "./invocation_filter";
import InvocationInProgressComponent from "./invocation_in_progress";
import InvocationModel from "./invocation_model";
import WorkflowCommands from "./workflow_commands";
import InvocationNotFoundComponent from "./invocation_not_found";
import InvocationOverviewComponent from "./invocation_overview";
import RawLogsCardComponent from "./invocation_raw_logs_card";
import InvocationTabsComponent, { getActiveTab } from "./invocation_tabs";
import TimingCardComponent from "./invocation_timing_card";
import ExecutionCardComponent from "./invocation_execution_card";
import ActionCardComponent from "./invocation_action_card";
import TargetsComponent from "./invocation_targets";
import { BuildBuddyError } from "../util/errors";

interface State {
  loading: boolean;
  inProgress: boolean;
  error: BuildBuddyError | null;

  model: InvocationModel;
}

interface Props {
  user?: User;
  invocationId: string;
  hash: string;
  search: URLSearchParams;
  denseMode: boolean;
}

const largePageSize = 100;
const smallPageSize = 10;

export default class InvocationComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    inProgress: false,
    error: null,

    model: new InvocationModel(),
  };

  timeoutRef: number;

  componentWillMount() {
    document.title = `Invocation ${this.props.invocationId} | BuildBuddy`;
    // TODO(siggisim): Move moment configuration elsewhere
    moment.relativeTimeThreshold("ss", 0);

    this.fetchInvocation();
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  fetchInvocation() {
    let request = new invocation.GetInvocationRequest();
    request.lookup = new invocation.InvocationLookup();
    request.lookup.invocationId = this.props.invocationId;
    rpcService.service
      .getInvocation(request)
      .then((response: invocation.GetInvocationResponse) => {
        console.log(response);
        let showInProgressScreen = false;
        if (
          response.invocation.length &&
          response.invocation[0].invocationStatus == invocation.Invocation.InvocationStatus.PARTIAL_INVOCATION_STATUS
        ) {
          showInProgressScreen = response.invocation[0].event.length == 0;
          this.fetchUpdatedProgress();
        }
        this.setState({
          inProgress: showInProgressScreen,
          model: InvocationModel.modelFromInvocations(response.invocation as invocation.Invocation[]),
          loading: false,
        });
        document.title = `${this.state.model.getUser(
          true
        )} ${this.state.model.getCommand()} ${this.state.model.getPattern()} | BuildBuddy`;
        faviconService.setFaviconForType(this.state.model.getFaviconType());
      })
      .catch((error: any) => {
        console.error(error);
        this.setState({
          error: BuildBuddyError.parse(error),
          loading: false,
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
    if (this.state.loading || this.props.user === undefined) {
      return <div className="loading"></div>;
    }

    if (this.state.error) {
      return (
        <InvocationNotFoundComponent
          invocationId={this.props.invocationId}
          error={this.state.error}
          user={this.props.user}
        />
      );
    }

    if (this.state.inProgress) {
      return <InvocationInProgressComponent invocationId={this.props.invocationId} />;
    }

    let targetLabel = this.props.search.get("target");
    if (targetLabel) {
      let completed = this.state.model.completedMap.get(targetLabel);
      let actionEvents =
        completed?.buildEvent.children
          .flatMap((child) =>
            this.state.model.actionMap
              .get(child?.actionCompleted?.label)
              ?.filter(
                (event) =>
                  event?.buildEvent?.id?.actionCompleted?.primaryOutput == child?.actionCompleted?.primaryOutput &&
                  event?.buildEvent?.id?.actionCompleted?.configuration?.id == child?.actionCompleted?.configuration?.id
              )
          )
          .filter((event) => !!event) || [];

      return (
        <TargetComponent
          invocationId={this.props.invocationId}
          hash={this.props.hash}
          configuredEvent={this.state.model.configuredMap.get(targetLabel)}
          completedEvent={completed}
          testResultEvents={this.state.model.testResultMap.get(targetLabel)}
          testSummaryEvent={this.state.model.testSummaryMap.get(targetLabel)}
          actionEvents={actionEvents}
          user={this.props.user}
          targetLabel={targetLabel}
        />
      );
    }

    const activeTab = getActiveTab({
      hash: this.props.hash,
      role: this.state.model.getRole(),
      denseMode: this.props.denseMode,
    });
    const isBazelInvocation = this.state.model.isBazelInvocation();

    return (
      <div className="invocation">
        <div className={`shelf nopadding-dense ${this.state.model.getStatusClass()}`}>
          {this.props.denseMode ? (
            <DenseInvocationOverviewComponent
              user={this.props.user}
              invocationId={this.props.invocationId}
              model={this.state.model}
            />
          ) : (
            <InvocationOverviewComponent
              invocationId={this.props.invocationId}
              model={this.state.model}
              user={this.props.user}
            />
          )}
        </div>
        <div className="container nopadding-dense">
          <InvocationTabsComponent
            hash={this.props.hash}
            denseMode={this.props.denseMode}
            role={this.state.model.getRole()}
          />

          {(activeTab === "targets" || activeTab === "artifacts") && (
            <InvocationFilterComponent hash={activeTab} search={this.props.search} />
          )}

          {(activeTab === "all" || activeTab == "log") &&
            this.state.model.aborted?.aborted.description &&
            this.state.model.aborted.aborted.reason.toString().toLowerCase() != "skipped" && (
              <ErrorCardComponent model={this.state.model} />
            )}

          {this.state.model.workflowConfigured && (activeTab === "all" || activeTab === "commands") && (
            <WorkflowCommands model={this.state.model} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="failing"
              filter={this.props.search.get("targetFilter")}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
            />
          )}

          {(activeTab === "all" || activeTab == "log") && <SuggestionCardComponent model={this.state.model} />}

          {(activeTab === "all" || activeTab == "log") && this.state.model.isQuery() && (
            <QueryGraphCardComponent model={this.state.model} />
          )}

          {(activeTab === "all" || activeTab == "log") && (
            <BuildLogsCardComponent model={this.state.model} expanded={activeTab == "log"} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="passing"
              filter={this.props.search.get("targetFilter")}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
            />
          )}

          {(activeTab === "all" || activeTab == "details") && (
            <InvocationDetailsCardComponent model={this.state.model} limitResults={!activeTab} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "cache") && (
            <CacheCardComponent model={this.state.model} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "artifacts") && (
            <ArtifactsCardComponent
              model={this.state.model}
              filter={this.props.search.get("artifactFilter")}
              pageSize={activeTab ? largePageSize : smallPageSize}
            />
          )}

          {activeTab == "timing" && <TimingCardComponent model={this.state.model} />}

          {activeTab == "execution" && (
            <ExecutionCardComponent model={this.state.model} inProgress={this.state.inProgress} />
          )}

          {activeTab == "action" && <ActionCardComponent model={this.state.model} search={this.props.search} />}

          {activeTab == "fetches" && <FetchCardComponent model={this.state.model} inProgress={this.state.inProgress} />}

          {activeTab == "raw" && <RawLogsCardComponent model={this.state.model} pageSize={largePageSize} />}
        </div>
      </div>
    );
  }
}
