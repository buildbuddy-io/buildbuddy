import { PlayCircleIcon } from "lucide-react";
import moment from "moment";
import React from "react";
import { Subscription } from "rxjs";
import { api as api_common } from "../../proto/api/v1/common_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { google as google_grpc_code } from "../../proto/grpc_code_ts_proto";
import { google as google_grpc_status } from "../../proto/grpc_status_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import faviconService from "../favicon/favicon";
import UserPreferences from "../preferences/preferences";
import router from "../router/router";
import { Cancelable, CancelablePromise, default as rpcService } from "../service/rpc_service";
import shortcuts, { KeyCombo } from "../shortcuts/shortcuts";
import TargetComponent from "../target/target";
import TargetV2Component from "../target/target_v2";
import { BuildBuddyError } from "../util/errors";
import CacheRequestsCardComponent from "./cache_requests_card";
import ChildInvocations from "./child_invocations";
import DenseInvocationOverviewComponent from "./dense/dense_invocation_overview";
import { ExecuteOperation, ExecutionStage, executionStatusLabel, waitExecution } from "./execution_status";
import InvocationActionCardComponent from "./invocation_action_card";
import ArtifactsCardComponent from "./invocation_artifacts_card";
import { InvocationBotCard } from "./invocation_bot_card";
import BuildLogsCardComponent from "./invocation_build_logs_card";
import CacheCardComponent from "./invocation_cache_card";
import InvocationCoverageCardComponent from "./invocation_coverage_card";
import InvocationDetailsCardComponent from "./invocation_details_card";
import ErrorCardComponent from "./invocation_error_card";
import InvocationExecLogCardComponent from "./invocation_exec_log_card";
import FetchCardComponent from "./invocation_fetch_card";
import FileCardComponent from "./invocation_file_card";
import InvocationFilterComponent from "./invocation_filter";
import InvocationInProgressComponent from "./invocation_in_progress";
import InvocationLogsModel from "./invocation_logs_model";
import InvocationModel, { CI_RUNNER_ROLE } from "./invocation_model";
import InvocationNotFoundComponent from "./invocation_not_found";
import InvocationOverviewComponent from "./invocation_overview";
import QueryGraphCardComponent from "./invocation_query_graph_card";
import RawLogsCardComponent from "./invocation_raw_logs_card";
import SpawnCardComponent from "./invocation_spawn_card";
import SuggestionCardComponent, { getSuggestions } from "./invocation_suggestion_card";
import InvocationTabsComponent, { getActiveTab } from "./invocation_tabs";
import TargetsComponent from "./invocation_targets";
import TimingCardComponent from "./invocation_timing_card";
import ScorecardCardComponent from "./scorecard_card";

interface State {
  loading: boolean;
  error: BuildBuddyError | null;

  model?: InvocationModel;

  /**
   * The CI runner execution responsible for creating this invocation, if
   * applicable.
   */
  runnerExecution?: execution_stats.Execution;
  runnerLastExecuteOperation?: ExecuteOperation;

  /*
   * We only need to update the child invocation cards right when they've started and ended.
   * Memoize them on the client, so we don't need to keep fetching them from the
   * db in the meantime.
   */
  childInvocations: invocation.Invocation[];

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
    error: null,
    keyboardShortcutHandle: "",
    childInvocations: [],
  };

  private timeoutRef: number | undefined;
  private logsModel?: InvocationLogsModel;
  private logsSubscription?: Subscription;
  private modelChangedSubscription?: Subscription;
  private runnerExecutionRPC?: CancelablePromise;
  private cancelGroupIdOverride?: () => void;

  private seenChildInvocationConfiguredIds = new Set<string>();
  private seenChildInvocationCompletedIds = new Set<string>();

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
    if (!this.isQueued() && !this.props.search.get("runnerFailed")) {
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

    if (this.props.search.get("openChild") == "true" && this.state.childInvocations.length > 0) {
      const childInvocationId = this.state.childInvocations[0].invocationId;
      const url = `/invocation/${childInvocationId}?queued=true`;
      router.replaceURL(url);
    }

    // Update title and favicon
    if (this.state.model) {
      document.title = `${this.state.model.getUserPossessivePrefix()}${this.state.model.getCommand()} ${this.state.model.getPattern()} | BuildBuddy`;
      faviconService.setFaviconForType(this.state.model.getFaviconType());
    }
    // If in progress or queued, schedule another fetch of the invocation (and
    // runner execution if applicable).
    if (this.state.model?.isInProgress() || this.isQueued()) {
      this.scheduleRefetch();
    } else if (!prevState.model && this.state.model && this.isCIRunnerBuild() && !this.state.runnerExecution) {
      // If we just learned that this is a CI runner build then fetch the runner
      // execution, since we won't have scheduled a refetch (which includes both
      // the invocation and runner execution).
      this.fetchRunnerExecution();
    }
    // If we don't have an invocation yet, stream updates from the runner
    // execution so we can see what it's doing before the invocation is created.
    if (prevState.runnerExecution?.executionId !== this.state.runnerExecution?.executionId && !this.state.model) {
      this.runnerExecutionStream?.cancel();
      if (this.state.runnerExecution?.executionId) {
        this.streamRunnerExecution();
      }
    } else if (this.state.model) {
      this.runnerExecutionStream?.cancel();
    }

    // If we transitioned from queued to not queued, and we have an invocation,
    // start fetching logs for the workflow.
    if (this.isQueued(prevProps, prevState) && !this.isQueued() && this.state.model) {
      this.logsModel?.startFetching();
    }
    // If we have an invocation, or we failed to fetch an invocation and the CI
    // runner failed, we're no longer queued.
    if (
      this.isQueued() &&
      (this.state.model || (this.state.error && this.hasStatusError(this.state.runnerExecution)))
    ) {
      router.setQueryParam("queued", null);
      if (this.state.error && this.hasStatusError(this.state.runnerExecution)) {
        // Make sure we preserve info about this being a failed runner execution
        // if the page is refreshed.
        router.setQueryParam("runnerFailed", "true");
      }
    }
  }

  componentWillUnmount() {
    if (this.timeoutRef) {
      clearTimeout(this.timeoutRef);
    }
    this.logsModel?.stopFetching();
    this.logsSubscription?.unsubscribe();
    this.runnerExecutionRPC?.cancel();
    shortcuts.deregister(this.state.keyboardShortcutHandle);

    this.cancelGroupIdOverride?.();
    this.cancelGroupIdOverride = undefined;
  }

  /**
   * Temporarily overrides the requestContext used by the RPC service to set the
   * preferred group ID to the invocation owner group ID, even if that group is
   * not currently selected in the group picker. Does nothing if the user does
   * not have access to the invocation owner group ID.
   *
   * This is required since cache artifacts are group-specific, and various
   * subcomponents of the invocation page will need to fetch cache artifacts
   * associated with the invocation.
   */
  updateGroupIdOverride(model: InvocationModel) {
    this.cancelGroupIdOverride?.();
    this.cancelGroupIdOverride = undefined;
    const invocationGroupId = model.invocation?.acl?.groupId;
    if (invocationGroupId && this.props.user?.groups?.some((g) => g.id === invocationGroupId)) {
      this.cancelGroupIdOverride = rpcService.overrideGroupId(invocationGroupId);
    }
  }

  async fetchInvocation() {
    // If applicable, fetch the CI runner execution in parallel. The CI runner
    // execution is what creates the invocation, so it can give us some
    // diagnostic info in the case where the invocation is never created, and
    // can also let us show better suggestions for the invocation.
    if (this.isCIRunnerBuild()) {
      this.fetchRunnerExecution();
    }

    const fetchChildren = this.shouldFetchChildren(this.state.model);
    let request = new invocation.GetInvocationRequest();
    request.lookup = new invocation.InvocationLookup();
    request.lookup.invocationId = this.props.invocationId;
    request.lookup.fetchChildInvocations = fetchChildren;
    return rpcService.service
      .getInvocation(request)
      .then((response: invocation.GetInvocationResponse) => {
        console.log(response);
        if (!response.invocation || response.invocation.length === 0) {
          throw new BuildBuddyError("NotFound", "Invocation not found.");
        }
        const inv = response.invocation[0];
        const model = new InvocationModel(inv);
        this.updateGroupIdOverride(model);

        // Only update the child invocations if we've fetched new updates.
        const childInvocations = fetchChildren ? inv.childInvocations : this.state.childInvocations;
        this.setState({
          model: model,
          error: null,
          childInvocations: childInvocations,
        });

        if (fetchChildren) {
          for (let child of childInvocations) {
            this.seenChildInvocationConfiguredIds.add(child.invocationId);
          }
        }
      })
      .catch((error: any) => {
        console.error("Failed to fetch invocation:", error);
        this.setState({ error: BuildBuddyError.parse(error) });
      })
      .finally(() => this.setState({ loading: false }));
  }

  shouldFetchChildren(model: InvocationModel | undefined): boolean {
    if (!model) return true;
    const childInvocationConfiguredEvents = model.childInvocationsConfigured;
    let shouldFetch = false;

    for (const event of childInvocationConfiguredEvents) {
      for (let inv of event.invocation) {
        if (!this.seenChildInvocationConfiguredIds.has(inv.invocationId)) {
          shouldFetch = true;
        }
      }
    }

    for (const iid of model.childInvocationCompletedByInvocationId.keys()) {
      if (!this.seenChildInvocationCompletedIds.has(iid)) {
        this.seenChildInvocationCompletedIds.add(iid);
        shouldFetch = true;
      }
    }

    return shouldFetch;
  }

  scheduleRefetch() {
    // Unless there is already a pending fetch, refetch invocation data
    // every 3 seconds to update status.
    this.timeoutRef ??= window.setTimeout(async () => {
      // Before fetching the invocation, wait for the runner execution to be
      // fetched, so we don't keep canceling the execution fetch if it takes
      // longer than the invocation poll interval.
      if (this.runnerExecutionRPC) await this.runnerExecutionRPC;

      await this.fetchInvocation();
      this.timeoutRef = undefined;
    }, 3000);
  }

  getBuildLogs(model: InvocationModel): string {
    if (!model.hasChunkedEventLogs()) {
      // Use the inlined console buffer if this invocation was created before
      // log chunking existed.
      return model.invocation.consoleBuffer;
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

  isCIRunnerBuild() {
    return Boolean(
      this.props.search.get("queued") ||
        this.props.search.get("runnerFailed") ||
        this.state.model?.getRole() === CI_RUNNER_ROLE
    );
  }

  fetchRunnerExecution() {
    this.runnerExecutionRPC?.cancel();
    this.runnerExecutionRPC = rpcService.service
      .getExecution({
        executionLookup: new execution_stats.ExecutionLookup({
          invocationId: this.props.invocationId,
        }),
        // Fetch the full ExecuteResponse, not just metadata.
        inlineExecuteResponse: true,
      })
      .then((response) => {
        const runnerExecution = response.execution?.[response.execution.length - 1] ?? undefined;
        this.setState({ runnerExecution });
      });
    return this.runnerExecutionRPC;
  }

  private runnerExecutionStream?: Cancelable;

  streamRunnerExecution() {
    if (!capabilities.config.streamingHttpEnabled) return;

    const runnerExecution = this.state.runnerExecution;
    if (!runnerExecution?.executionId) return;

    // We technically know that we won't have the invocation at this point,
    // but at least flagging this call here will get the default entry and
    // ensure that we try to keep this matching region in the future (if we
    // ever wind up needing it).
    const service = rpcService.getRegionalServiceOrDefault(
      this.state.model?.stringCommandLineOption("remote_execution") ?? ""
    );

    this.runnerExecutionStream = waitExecution(service, runnerExecution.executionId, {
      next: (op) => {
        this.setState({ runnerLastExecuteOperation: op });
        if (op.response) {
          this.setState({
            runnerExecution: new execution_stats.Execution({
              ...runnerExecution,
              executeResponse: op.response,
            }),
          });
        }
      },
      error: (error) => {
        console.error("Failed to fetch runner execution", error);
      },
      complete: () => {},
    });
  }

  renderTargetPage(targetLabel: string, targetStatus: api_common.v1.Status) {
    if (!this.state.model) return null;

    if (this.state.model.invocation.targetGroups.length) {
      // Invocation events are paginated; use the new target component to fetch
      // the target data.
      return (
        <TargetV2Component
          label={targetLabel}
          status={targetStatus}
          invocationId={this.props.invocationId}
          tab={this.props.tab}
          user={this.props.user}
          repo={this.state.model.getGithubRepo()}
          commit={this.state.model.getCommit()}
          dark={!this.props.preferences.lightTerminalEnabled}
          model={this.state.model}
          search={this.props.search}
        />
      );
    }

    let completed = this.state.model.completedMap.get(targetLabel);
    let actionEvents: invocation.InvocationEvent[] =
      completed?.buildEvent?.children
        .flatMap((child) =>
          (this.state.model!.actionMap.get(child?.actionCompleted?.label ?? "") ?? []).filter(
            (event) =>
              event?.buildEvent?.id?.actionCompleted?.primaryOutput === child?.actionCompleted?.primaryOutput &&
              event?.buildEvent?.id?.actionCompleted?.configuration?.id === child?.actionCompleted?.configuration?.id
          )
        )
        .filter((event) => !!event) || [];
    return (
      <TargetComponent
        model={this.state.model}
        invocationId={this.props.invocationId}
        tab={this.props.tab}
        files={completed?.buildEvent ? (this.state.model.getFiles(completed.buildEvent) ?? []) : []}
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

  hasStatusError(execution: execution_stats.Execution | undefined) {
    if (!execution) return false;
    return execution.status?.code !== google_grpc_code.rpc.Code.OK;
  }

  formatStatus(status: google_grpc_status.rpc.Status) {
    const codeName = google_grpc_code.rpc.Code[status.code];
    return `Error: ${status.message} (code: ${codeName})`;
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" debug-id="invocation-loading"></div>;
    }

    // If we don't have an invocation but we have an execution error, show just
    // the execution error.
    if (!this.state.model && this.hasStatusError(this.state.runnerExecution)) {
      return (
        <InvocationInProgressComponent
          invocationId={this.props.invocationId}
          title={"Invocation execution failed"}
          subtitle={
            <span className="error-text">
              {/* Render ExecuteResponse status if still in cache, or fall back to truncated status from DB. */}
              {(this.state.runnerExecution?.executeResponse?.status &&
                this.formatStatus(this.state.runnerExecution.executeResponse.status)) ||
                (this.state.runnerExecution?.status && this.formatStatus(this.state.runnerExecution.status)) ||
                "unknown error"}
            </span>
          }
        />
      );
    }

    if (this.isQueued()) {
      const op = this.state.runnerLastExecuteOperation;
      return (
        <InvocationInProgressComponent
          invocationId={this.props.invocationId}
          title={
            !op || op.metadata?.stage === ExecutionStage.QUEUED
              ? "Invocation is waiting for an available worker..."
              : "Invocation in progress..."
          }
          subtitle={
            <div className="runner-execution-status">
              <PlayCircleIcon className="icon blue" />
              <span>{op ? executionStatusLabel(op) : "Queued"}...</span>
            </div>
          }
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

    // If we haven't received the invocation metadata yet (user, pattern, etc.),
    // render a generic "in progress" message.
    if (!this.state.model.isMetadataLoaded()) {
      return <InvocationInProgressComponent invocationId={this.props.invocationId} />;
    }

    let targetLabel = this.props.search.get("target");
    if (targetLabel) {
      const targetStatus = Number(this.props.search.get("targetStatus") || "0") as api_common.v1.Status;
      return this.renderTargetPage(targetLabel, targetStatus);
    }

    const activeTab = getActiveTab({
      tab: this.props.tab,
      role: this.state.model.getRole(),
      denseMode: this.props.preferences.denseModeEnabled,
    });
    const isBazelInvocation = this.state.model.isBazelInvocation();
    const fetchBuildLogs = () => {
      rpcService.downloadLog(this.props.invocationId, Number(this.state.model?.invocation.attempt ?? 0));
    };

    const suggestions = getSuggestions({
      model: this.state.model,
      buildLogs: this.getBuildLogs(this.state.model),
      runnerExecution: this.state.runnerExecution,
      user: this.props.user,
    });

    return (
      <div className="invocation">
        <div className={`shelf nopadding-dense ${this.state.model.getStatusClass()}`}>
          {this.props.preferences.denseModeEnabled ? (
            <DenseInvocationOverviewComponent user={this.props.user} model={this.state.model} />
          ) : (
            <InvocationOverviewComponent user={this.props.user} model={this.state.model} />
          )}
          {!isBazelInvocation && (
            <div className="container">
              <ChildInvocations childInvocations={this.state.childInvocations} />
            </div>
          )}
        </div>
        {!isBazelInvocation && (
          <div className="container">
            <div className="workflow-details-header">
              <h2>Run results</h2>
            </div>
          </div>
        )}
        <div className="container nopadding-dense">
          <InvocationTabsComponent
            tab={this.props.tab}
            denseMode={this.props.preferences.denseModeEnabled}
            role={this.state.model.getRole()}
            executionsEnabled={
              this.state.model.getIsRBEEnabled() ||
              this.state.model.isWorkflowInvocation() ||
              this.state.model.isHostedBazelInvocation()
            }
            hasCoverage={this.state.model.hasCoverage()}
            hasSuggestions={suggestions.length > 0}
            hasExecutionLogs={this.state.model.getIsExecutionLogEnabled()}
          />

          {(activeTab === "targets" ||
            activeTab === "artifacts" ||
            activeTab === "execution" ||
            activeTab == "spawns" ||
            activeTab == "files") && (
            <InvocationFilterComponent
              tab={this.props.tab}
              search={this.props.search}
              placeholder={activeTab === "execution" ? "Filter by target, action mnemonic, command, or digest..." : ""}
              // When serving a paginated invocation, debounce since searching
              // is done on the server.
              debounceMillis={this.state.model.invocation.targetGroups.length ? 200 : 0}
            />
          )}

          {(activeTab === "all" || activeTab === "log") && this.state.model.botSuggestions.length > 0 && (
            <InvocationBotCard suggestions={this.state.model.botSuggestions} />
          )}

          {(activeTab === "all" || activeTab === "log") && <ErrorCardComponent model={this.state.model} />}

          {isBazelInvocation && (activeTab === "all" || activeTab === "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="failing"
              filter={this.props.search.get("targetFilter") ?? ""}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
              showLoader
            />
          )}

          {(activeTab === "all" || activeTab === "log") && this.state.model.isQuery() && (
            <QueryGraphCardComponent buildLogs={this.getBuildLogs(this.state.model)} />
          )}

          {(activeTab === "all" || activeTab === "log") && (
            <BuildLogsCardComponent
              title={isBazelInvocation ? "Build logs" : "Runner logs"}
              dark={!this.props.preferences.lightTerminalEnabled}
              value={this.getBuildLogs(this.state.model)}
              loading={this.areBuildLogsLoading(this.state.model)}
              expanded={activeTab === "log"}
              fullLogsFetcher={fetchBuildLogs}
            />
          )}

          {(activeTab === "all" || activeTab === "log" || activeTab === "suggestions") && (
            <SuggestionCardComponent
              suggestions={suggestions}
              overview={activeTab !== "suggestions"}
              user={this.props.user}
            />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab === "targets") && (
            <TargetsComponent
              model={this.state.model}
              mode="passing"
              filter={this.props.search.get("targetFilter") ?? ""}
              pageSize={activeTab === "all" ? smallPageSize : largePageSize}
            />
          )}

          {(activeTab === "all" || activeTab === "details") && (
            <InvocationDetailsCardComponent model={this.state.model} limitResults={!activeTab} />
          )}

          {isBazelInvocation && (activeTab === "all" || activeTab === "cache") && (
            <CacheCardComponent model={this.state.model} />
          )}
          {isBazelInvocation &&
            (activeTab === "all" || activeTab === "cache") &&
            !capabilities.config.detailedCacheStatsEnabled && <ScorecardCardComponent model={this.state.model} />}
          {isBazelInvocation &&
            (activeTab === "all" || activeTab === "cache") &&
            capabilities.config.detailedCacheStatsEnabled && (
              <CacheRequestsCardComponent model={this.state.model} search={this.props.search} />
            )}

          {(activeTab === "all" || activeTab === "artifacts") && (
            <ArtifactsCardComponent
              model={this.state.model}
              filter={this.props.search.get("artifactFilter") ?? ""}
              pageSize={activeTab ? largePageSize : smallPageSize}
            />
          )}

          {activeTab === "execution" && (
            <InvocationExecLogCardComponent
              model={this.state.model}
              search={this.props.search}
              filter={this.props.search.get("executionFilter") ?? ""}
            />
          )}

          {activeTab === "spawns" && (
            <SpawnCardComponent
              model={this.state.model}
              search={this.props.search}
              filter={this.props.search.get("spawnFilter") ?? ""}
            />
          )}

          {activeTab === "files" && (
            <FileCardComponent
              model={this.state.model}
              search={this.props.search}
              filter={this.props.search.get("fileFilter") ?? ""}
            />
          )}

          {activeTab === "timing" && <TimingCardComponent model={this.state.model} />}

          {activeTab === "coverage" && <InvocationCoverageCardComponent model={this.state.model} />}

          {activeTab === "action" && (
            <InvocationActionCardComponent
              model={this.state.model}
              search={this.props.search}
              preferences={this.props.preferences}
            />
          )}

          {activeTab === "fetches" && <FetchCardComponent model={this.state.model} />}

          {activeTab === "raw" && <RawLogsCardComponent model={this.state.model} pageSize={largePageSize} />}
        </div>
      </div>
    );
  }
}
