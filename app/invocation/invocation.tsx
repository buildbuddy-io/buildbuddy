import moment from "moment";
import React from "react";
import { Subscription } from "rxjs";
import { invocation } from "../../proto/invocation_ts_proto";
import { invocation_status } from "../../proto/invocation_status_ts_proto";
import { User } from "../auth/auth_service";
import faviconService from "../favicon/favicon";
import rpcService from "../service/rpc_service";
import TargetComponent from "../target/target";
import DenseInvocationOverviewComponent from "./dense/dense_invocation_overview";
import ArtifactsCardComponent from "./invocation_artifacts_card";
import BuildLogsCardComponent from "./invocation_build_logs_card";
import QueryGraphCardComponent from "./invocation_query_graph_card";
import CacheCardComponent from "./invocation_cache_card";
import ScorecardCardComponent from "./scorecard_card";
import FetchCardComponent from "./invocation_fetch_card";
import InvocationDetailsCardComponent from "./invocation_details_card";
import ErrorCardComponent from "./invocation_error_card";
import SuggestionCardComponent, { getSuggestions } from "./invocation_suggestion_card";
import InvocationFilterComponent from "./invocation_filter";
import InvocationInProgressComponent from "./invocation_in_progress";
import InvocationModel from "./invocation_model";
import InvocationLogsModel from "./invocation_logs_model";
import ChildInvocations from "./child_invocations";
import InvocationNotFoundComponent from "./invocation_not_found";
import InvocationOverviewComponent from "./invocation_overview";
import RawLogsCardComponent from "./invocation_raw_logs_card";
import InvocationTabsComponent, { getActiveTab } from "./invocation_tabs";
import TimingCardComponent from "./invocation_timing_card";
import ExecutionCardComponent from "./invocation_execution_card";
import InvocationActionCardComponent from "./invocation_action_card";
import TargetsComponent from "./invocation_targets";
import { BuildBuddyError } from "../util/errors";
import UserPreferences from "../preferences/preferences";
import capabilities from "../capabilities/capabilities";
import CacheRequestsCardComponent from "./cache_requests_card";
import rpc_service from "../service/rpc_service";
import { InvocationBotCard } from "./invocation_bot_card";

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
  preferences: UserPreferences;
}

const largePageSize = 100;
const smallPageSize = 10;

let modelChangedSubscription: Subscription = undefined;
export default class InvocationComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    inProgress: false,
    error: null,

    model: new InvocationModel(),
  };

  private timeoutRef: number;
  private logsModel: InvocationLogsModel;
  private logsSubscription: Subscription;

  componentWillMount() {
    document.title = `Invocation ${this.props.invocationId} | BuildBuddy`;
    // TODO(siggisim): Move moment configuration elsewhere
    moment.relativeTimeThreshold("ss", 0);

    this.fetchInvocation();

    this.logsModel = new InvocationLogsModel(this.props.invocationId);
    // Re-render whenever we fetch new log chunks.
    this.logsSubscription = this.logsModel.onChange.subscribe({
      next: () => this.forceUpdate(),
    });
    this.logsModel.startFetching();
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
    this.logsModel?.stopFetching();
    this.logsSubscription?.unsubscribe();
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
          response.invocation[0].invocationStatus == invocation_status.InvocationStatus.PARTIAL_INVOCATION_STATUS
        ) {
          showInProgressScreen = response.invocation[0].event.length == 0;
          this.fetchUpdatedProgress();
        }
        if (modelChangedSubscription) {
          modelChangedSubscription.unsubscribe();
        }
        let model = InvocationModel.modelFromInvocations(response.invocation as invocation.Invocation[]);
        modelChangedSubscription = model.onChange.subscribe(() => {
          this.setState({ model: this.state.model });
        });
        this.setState({
          inProgress: showInProgressScreen,
          model: model,
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
    this.timeoutRef = window.setTimeout(() => {
      this.fetchInvocation();
    }, 3000);
  }

  getBuildLogs() {
    if (!this.state.model.hasChunkedEventLogs()) {
      // Use the inlined console buffer if this invocation was created before
      // log chunking existed.
      return this.state.model.invocations[0]?.consoleBuffer;
    }
    return this.logsModel.getLogs();
  }

  areBuildLogsLoading() {
    if (!this.state.model.hasChunkedEventLogs()) {
      return false;
    }
    return this.logsModel.isFetching() && !this.logsModel.getLogs();
  }

  render() {
    if (this.state.loading) {
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
          model={this.state.model}
          invocationId={this.props.invocationId}
          hash={this.props.hash}
          files={this.state.model.getFiles(completed?.buildEvent)}
          configuredEvent={this.state.model.configuredMap.get(targetLabel)}
          skippedEvent={this.state.model.skippedMap.get(targetLabel)}
          completedEvent={completed}
          testResultEvents={this.state.model.testResultMap.get(targetLabel)}
          testSummaryEvent={this.state.model.testSummaryMap.get(targetLabel)}
          actionEvents={actionEvents}
          user={this.props.user}
          repo={this.state.model.getGithubRepo()}
          targetLabel={targetLabel}
          dark={!this.props.preferences.lightTerminalEnabled}
        />
      );
    }

    const activeTab = getActiveTab({
      hash: this.props.hash,
      role: this.state.model.getRole(),
      denseMode: this.props.preferences.denseModeEnabled,
    });
    const isBazelInvocation = this.state.model.isBazelInvocation();
    const fetchBuildLogs = () => {
      rpc_service.downloadLog(this.props.invocationId, Number(this.state.model.invocations[0]?.attempt ?? 0));
    };

    const suggestions = getSuggestions({
      model: this.state.model,
      buildLogs: this.getBuildLogs(),
      user: this.props.user,
    });

    return (
      <div className="invocation">
        <div className={`shelf nopadding-dense ${this.state.model.getStatusClass()}`}>
          {this.props.preferences.denseModeEnabled ? (
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
            denseMode={this.props.preferences.denseModeEnabled}
            role={this.state.model.getRole()}
            executionsEnabled={this.state.model.getIsRBEEnabled() || this.state.model.isWorkflowInvocation()}
            hasSuggestions={suggestions.length > 0}
          />

          {(activeTab === "targets" || activeTab === "artifacts" || activeTab === "execution") && (
            <InvocationFilterComponent
              hash={this.props.hash}
              search={this.props.search}
              placeholder={activeTab == "execution" ? "Filter by digest or command..." : ""}
            />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.aborted?.aborted.description && (
            <ErrorCardComponent model={this.state.model} />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.botSuggestions.length > 0 && (
            <InvocationBotCard suggestions={this.state.model.botSuggestions} />
          )}

          {(this.state.model.workflowConfigured || this.state.model.childInvocationsConfigured) &&
            (activeTab === "all" || activeTab === "commands") && <ChildInvocations model={this.state.model} />}

          {isBazelInvocation && (activeTab === "all" || activeTab == "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="failing"
              filter={this.props.search.get("targetFilter")}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
            />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.isQuery() && (
            <QueryGraphCardComponent buildLogs={this.getBuildLogs()} />
          )}

          {(activeTab === "all" || activeTab == "log") && (
            <BuildLogsCardComponent
              dark={!this.props.preferences.lightTerminalEnabled}
              value={this.getBuildLogs()}
              loading={this.areBuildLogsLoading()}
              expanded={activeTab == "log"}
              fullLogsFetcher={fetchBuildLogs}
            />
          )}

          {(activeTab === "all" || activeTab == "log" || activeTab === "suggestions") && (
            <SuggestionCardComponent
              suggestions={suggestions}
              overview={activeTab !== "suggestions"}
              user={this.props.user}
            />
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
          {isBazelInvocation &&
            (activeTab === "all" || activeTab == "cache") &&
            !capabilities.config.detailedCacheStatsEnabled && <ScorecardCardComponent model={this.state.model} />}
          {isBazelInvocation &&
            (activeTab === "all" || activeTab == "cache") &&
            capabilities.config.detailedCacheStatsEnabled && (
              <CacheRequestsCardComponent model={this.state.model} search={this.props.search} />
            )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "artifacts") && (
            <ArtifactsCardComponent
              model={this.state.model}
              filter={this.props.search.get("artifactFilter")}
              pageSize={activeTab ? largePageSize : smallPageSize}
            />
          )}

          {activeTab == "execution" && (
            <ExecutionCardComponent
              model={this.state.model}
              inProgress={this.state.inProgress}
              search={this.props.search}
              filter={this.props.search.get("executionFilter")}
            />
          )}

          {activeTab == "timing" && <TimingCardComponent model={this.state.model} />}

          {activeTab == "action" && (
            <InvocationActionCardComponent
              model={this.state.model}
              search={this.props.search}
              preferences={this.props.preferences}
            />
          )}

          {activeTab == "fetches" && <FetchCardComponent model={this.state.model} inProgress={this.state.inProgress} />}

          {activeTab == "raw" && <RawLogsCardComponent model={this.state.model} pageSize={largePageSize} />}
        </div>
      </div>
    );
  }
}
