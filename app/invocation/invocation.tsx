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
import shortcuts, { KeyCombo } from "../shortcuts/shortcuts";
import router from "../router/router";
import rpc_service from "../service/rpc_service";
import { InvocationBotCard } from "./invocation_bot_card";

interface State {
  loading: boolean;
  inProgress: boolean;
  error: BuildBuddyError | null;

  model?: InvocationModel;

  keyboardShortcutHandle: string;
}

interface Props {
  user?: User;
  invocationId: string;
  tab: string;
  search: URLSearchParams;
  preferences: UserPreferences;
}

const largePageSize = 100;
const smallPageSize = 10;

export default class InvocationComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    inProgress: false,
    error: null,
    keyboardShortcutHandle: "",
  };

  private timeoutRef: number = 0;
  private logsModel?: InvocationLogsModel;
  private logsSubscription?: Subscription;
  private modelChangedSubscription?: Subscription;

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
    if (!this.isQueued()) {
      this.logsModel.startFetching();
    }
  }

  componentDidMount() {
    let handle = shortcuts.register(KeyCombo.u, () => {
      // Used to select the correct invocation on the history page so that
      // selecting an invocation with 'enter' and then going back with 'u' ends
      // up with the same invocation still selected.
      localStorage["selected_invocation_id"] = this.props.invocationId;
      router.navigateHome();
    });
    this.setState({ keyboardShortcutHandle: handle });
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    // Update model subscription
    if (this.state.model !== prevState.model) {
      this.modelChangedSubscription?.unsubscribe();
      if (this.state.model) {
        this.modelChangedSubscription = this.state.model?.onChange.subscribe(() => this.forceUpdate());
      }
    }
    // Update title and favicon
    if (this.state.model) {
      document.title = `${this.state.model.getUser(
        true
      )} ${this.state.model.getCommand()} ${this.state.model.getPattern()} | BuildBuddy`;
      faviconService.setFaviconForType(this.state.model.getFaviconType());
    }
    // If in progress or queued, schedule another fetch
    if (this.state.model?.isInProgress() || this.isQueued()) {
      this.scheduleRefetch();
    }
    // If we transitioned from queued to not queued, start fetching logs.
    if (this.isQueued(prevProps, prevState) && !this.isQueued()) {
      this.logsModel?.startFetching();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
    this.logsModel?.stopFetching();
    this.logsSubscription?.unsubscribe();
    shortcuts.deregister(this.state.keyboardShortcutHandle);
  }

  fetchInvocation() {
    const wasQueued = this.isQueued();
    let request = new invocation.GetInvocationRequest();
    request.lookup = new invocation.InvocationLookup();
    request.lookup.invocationId = this.props.invocationId;
    rpcService.service
      .getInvocation(request)
      .then((response: invocation.GetInvocationResponse) => {
        console.log(response);
        if (!response.invocation || response.invocation.length === 0) {
          throw new BuildBuddyError("NotFound", "Invocation not found.");
        }
        const model = InvocationModel.modelFromInvocations(response.invocation[0], response.invocation.slice(1));
        // Only show the in-progress screen if we don't have any events yet.
        const showInProgressScreen = model.isInProgress() && !response.invocation[0].event?.length;
        this.setState({
          inProgress: showInProgressScreen,
          model: model,
          error: null,
        });
        if (wasQueued) {
          router.setQueryParam("queued", null);
        }
      })
      .catch((error: any) => {
        console.error(error);
        this.setState({ error: BuildBuddyError.parse(error) });
      })
      .finally(() => this.setState({ loading: false }));
  }

  scheduleRefetch() {
    clearTimeout(this.timeoutRef);
    // Refetch invocation data in 3 seconds to update status.
    this.timeoutRef = window.setTimeout(() => {
      this.fetchInvocation();
    }, 3000);
  }

  getBuildLogs(model: InvocationModel): string {
    if (!model.hasChunkedEventLogs()) {
      // Use the inlined console buffer if this invocation was created before
      // log chunking existed.
      return model.getPrimaryInvocation().consoleBuffer;
    }
    return this.logsModel?.getLogs() ?? "";
  }

  areBuildLogsLoading(model: InvocationModel) {
    if (!model.hasChunkedEventLogs()) {
      return false;
    }
    return Boolean(this.logsModel?.isFetching() && !this.logsModel?.getLogs());
  }

  isQueued(props = this.props, state = this.state) {
    return !state.model && props.search.get("queued") === "true";
  }

  render() {
    if (this.state.loading) {
      return <div className="loading"></div>;
    }

    if (this.isQueued()) {
      return (
        <InvocationInProgressComponent
          invocationId={this.props.invocationId}
          title={"Invocation is waiting for an available worker..."}
        />
      );
    }

    if (this.state.error || !this.state.model) {
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
      let actionEvents: invocation.InvocationEvent[] =
        completed?.buildEvent?.children
          .flatMap((child) =>
            (this.state.model!.actionMap.get(child?.actionCompleted?.label ?? "") ?? []).filter(
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
          tab={this.props.tab}
          files={completed?.buildEvent ? this.state.model.getFiles(completed.buildEvent) ?? [] : []}
          configuredEvent={this.state.model.configuredMap.get(targetLabel)}
          skippedEvent={this.state.model.skippedMap.get(targetLabel)}
          completedEvent={completed}
          testResultEvents={this.state.model.testResultMap.get(targetLabel) ?? []}
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
      tab: this.props.tab,
      role: this.state.model.getRole(),
      denseMode: this.props.preferences.denseModeEnabled,
    });
    const isBazelInvocation = this.state.model.isBazelInvocation();
    const fetchBuildLogs = () => {
      rpc_service.downloadLog(this.props.invocationId, Number(this.state.model?.getPrimaryInvocation().attempt ?? 0));
    };

    const suggestions = getSuggestions({
      model: this.state.model,
      buildLogs: this.getBuildLogs(this.state.model),
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
          {!isBazelInvocation && (
            <div className="container">
              <ChildInvocations model={this.state.model} />
            </div>
          )}
        </div>
        {!isBazelInvocation && (
          <div className="container">
            <div className="workflow-details-header">
              <h2>Runner results (advanced)</h2>
              <div>Raw output from the runner instance that executed the Bazel commands.</div>
            </div>
          </div>
        )}
        <div className="container nopadding-dense">
          <InvocationTabsComponent
            tab={this.props.tab}
            denseMode={this.props.preferences.denseModeEnabled}
            role={this.state.model.getRole()}
            executionsEnabled={this.state.model.getIsRBEEnabled() || this.state.model.isWorkflowInvocation()}
            hasSuggestions={suggestions.length > 0}
          />

          {(activeTab === "targets" || activeTab === "artifacts" || activeTab === "execution") && (
            <InvocationFilterComponent
              tab={this.props.tab}
              search={this.props.search}
              placeholder={activeTab == "execution" ? "Filter by digest or command..." : ""}
            />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.aborted?.aborted?.description && (
            <ErrorCardComponent model={this.state.model} />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.botSuggestions.length > 0 && (
            <InvocationBotCard suggestions={this.state.model.botSuggestions} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab == "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="failing"
              filter={this.props.search.get("targetFilter") ?? ""}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
            />
          )}

          {(activeTab === "all" || activeTab == "log") && this.state.model.isQuery() && (
            <QueryGraphCardComponent buildLogs={this.getBuildLogs(this.state.model)} />
          )}

          {(activeTab === "all" || activeTab == "log") && (
            <BuildLogsCardComponent
              title={isBazelInvocation ? "Build logs" : "Runner logs"}
              dark={!this.props.preferences.lightTerminalEnabled}
              value={this.getBuildLogs(this.state.model)}
              loading={this.areBuildLogsLoading(this.state.model)}
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
              filter={this.props.search.get("targetFilter") ?? ""}
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
              filter={this.props.search.get("artifactFilter") ?? ""}
              pageSize={activeTab ? largePageSize : smallPageSize}
            />
          )}

          {activeTab == "execution" && (
            <ExecutionCardComponent
              model={this.state.model}
              inProgress={this.state.inProgress}
              search={this.props.search}
              filter={this.props.search.get("executionFilter") ?? ""}
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
