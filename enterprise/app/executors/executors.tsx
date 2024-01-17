import React from "react";
import Banner from "../../../app/components/banner/banner";
import rpcService from "../../../app/service/rpc_service";
import {BuildBuddyError} from "../../../app/util/errors";
import {User} from "../../../app/auth/auth_service";
import {scheduler} from "../../../proto/scheduler_ts_proto";
import ExecutorCardComponent from "./executor_card";
import {Subscription} from "rxjs";
import {api_key} from "../../../proto/api_key_ts_proto";
import {bazel_config} from "../../../proto/bazel_config_ts_proto";
import router from "../../../app/router/router";
import Select, {Option} from "../../../app/components/select/select";
import LinkButton from "../../../app/components/button/link_button";
import {Cpu, Globe, Hash, Laptop, LucideIcon} from "lucide-react";
import {scheduler_queue} from "../../../proto/scheduler_queue_ts_proto";
import {durationMillis} from "../../../app/format/format";
import {build} from "../../../proto/remote_execution_ts_proto";
import error_service from "../../../app/errors/error_service";
import {timestampToDate} from "../../../app/util/proto";
import Value = build.bazel.remote.execution.v2.ExecutionStage.Value;
import Link from "../../../app/components/link/link";
import Button from "../../../app/components/button/button";
import {invocation} from "../../../proto/invocation_ts_proto";

enum FetchType {
  Executors,
  ApiKeys,
  BazelConfig,
}

interface ExecutorDeployProps {
  executorKeys: api_key.IApiKey[];
  schedulerUri: string;
}

interface ExecutorDeployState {
  selectedExecutorKeyIdx: number;
}
class ExecutorDeploy extends React.Component<ExecutorDeployProps, ExecutorDeployState> {
  state = { selectedExecutorKeyIdx: 0 };

  onSelectExecutorApiKey(e: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ selectedExecutorKeyIdx: Number(e.target.value) });
  }

  render() {
    return (
      <>
        <p>Self-hosted executors can be deployed by running a simple Docker image on any machine.</p>
        <p>The example below shows how to run an executor manually using the Docker CLI.</p>
        API key:
        <Select
          title="Select API key"
          className="credential-picker"
          name="selectedCredential"
          value={this.state.selectedExecutorKeyIdx}
          onChange={this.onSelectExecutorApiKey.bind(this)}>
          {this.props.executorKeys.map((key, index) => (
            <Option key={index} value={index}>
              {key.label || "Untitled key"} - {key.value}
            </Option>
          ))}
        </Select>
        <code>
          <pre>
            {`docker run \\
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \\
    --volume /tmp/buildbuddy:/buildbuddy \\
    gcr.io/flame-public/buildbuddy-executor-enterprise:latest \\
    --executor.docker_socket=/var/run/docker.sock \\
    --executor.host_root_directory=/tmp/buildbuddy \\
    --executor.app_target=${this.props.schedulerUri} \\
    --executor.api_key=${this.props.executorKeys[this.state.selectedExecutorKeyIdx]?.value}`}
          </pre>
        </code>
      </>
    );
  }
}

interface ExecutorSetupProps {
  user: User;
  executorKeys: api_key.IApiKey[];
  executors: scheduler.GetExecutionNodesResponse.Executor[];
  schedulerUri: string;
}

class ExecutorSetup extends React.Component<ExecutorSetupProps> {
  state = { selectedExecutorKeyIdx: 0 };

  render() {
    // If the user already has executors running, show the "short" version of the setup.
    if (this.props.user.selectedGroup.useGroupOwnedExecutors && this.props.executors.length) {
      return (
        <>
          <h2>Deploy additional executors</h2>
          <ExecutorDeploy executorKeys={this.props.executorKeys} schedulerUri={this.props.schedulerUri} />
        </>
      );
    }

    return (
      <>
        <h1>Set up self-hosted executors</h1>
        <hr />
        <h2>1. Create an API key for executor registration</h2>
        {this.props.executorKeys.length == 0 && (
          <div>
            <p>There are no API keys with the executor capability configured for your organization.</p>
            <p>API keys are used to authorize self-hosted executors.</p>
            {this.props.user.canCall("createApiKey") && (
              <LinkButton href="/settings/org/api-keys" className="manage-keys-button">
                Manage keys
              </LinkButton>
            )}
          </div>
        )}
        {this.props.executorKeys.length > 0 && (
          <>
            <div>
              {this.props.executorKeys.length == 1 && <p>You have an Executor API key available.</p>}
              {this.props.executorKeys.length > 1 && (
                <p>You have {this.props.executorKeys.length} Executor API keys available. </p>
              )}
              <p>These API keys can be used to deploy your executors.</p>
            </div>
            <h2>2. Deploy executors</h2>
            <ExecutorDeploy executorKeys={this.props.executorKeys} schedulerUri={this.props.schedulerUri} />
            <h2>3. Default to self-hosted executors in organization settings</h2>
            <p>Enable "Default to self-hosted executors" on the Organization Settings page.</p>
            <LinkButton href="/settings/" className="organization-settings-button">
              Open settings
            </LinkButton>
          </>
        )}
      </>
    );
  }
}

interface ExecutorsListProps {
  regions: { name: string; response: scheduler.GetExecutionNodesResponse }[];
}

interface ExecutorListState {
  selectedExecutorID?: string;
}

class ExecutorsList extends React.Component<ExecutorsListProps, ExecutorListState> {
  state: ExecutorListState = {
  }

  selectExecutor(executorID: string) {
    router.navigateToExecutor(executorID)
  }

  render() {
    if (this.state.selectedExecutorID) {
      return <ExecutorDetailsComponent executorID={this.state.selectedExecutorID} />
    }

    let executorsByPool = new Map<
      string,
      { region: string; executor: scheduler.GetExecutionNodesResponse.Executor }[]
    >();
    for (const r of this.props.regions) {
      for (const e of r.response.executor) {
        const key = r.name + "-" + (e.node?.os || "") + "-" + (e.node?.arch || "") + "-" + (e.node?.pool || "");
        if (!executorsByPool.has(key)) {
          executorsByPool.set(key, []);
        }
        executorsByPool.get(key)!.push({ region: r.name, executor: e });
      }
    }
    const keys = Array.from(executorsByPool.keys()).sort();

    return (
      <>
        <div className="executor-cards">
          {keys
            .map((key) => executorsByPool.get(key))
            .map((executors) => {
              if (!executors || executors.length == 0) {
                return null;
              }
              return (
                <>
                  <h2>{executors[0].executor.node?.pool || "Default Pool"}</h2>
                  <div className="executor-details">
                    {executors[0].region && (
                      <ExecutorDetail Icon={Globe} label="">
                        {[executors[0].region]}
                      </ExecutorDetail>
                    )}
                    <ExecutorDetail Icon={Hash} label="">
                      {executors.length} {executors.length === 1 ? "executor" : "executors"}
                    </ExecutorDetail>
                    <ExecutorDetail Icon={Laptop} label="OS">
                      {executors[0].executor.node?.os || "unknown"}
                    </ExecutorDetail>
                    <ExecutorDetail Icon={Cpu} label="Arch">
                      {executors[0].executor.node?.arch || "unknown"}
                    </ExecutorDetail>
                  </div>
                  {executors.length < 3 && (
                    <div>
                      <Banner type="warning" className="pool-size-warning">
                        For better performance and reliability, we suggest running a minimum of 3 executors per pool.
                      </Banner>
                    </div>
                  )}
                  {executors.map(
                    (node) =>
                      node.executor.node && (
                          <div onClick={this.selectExecutor.bind(this, node.executor.node.executorId)}>
                            <ExecutorCardComponent node={node.executor.node} isDefault={node.executor.isDefault} stats={node.executor.stats}/>
                          </div>
                      )
                  )}
                </>
              );
            })}
        </div>
      </>
    );
  }
}

function ExecutorDetail({ Icon, label, children }: { Icon: LucideIcon; label: string; children: React.ReactNode }) {
  return (
    <span className="executor-detail">
      <Icon className="icon" />
      <span>
        {label && <>{label}: </>}
        <b>{children}</b>
      </span>
    </span>
  );
}

interface EntryProps {
  entry: scheduler_queue.ExecutorQueueEntry;
}

class ExecutorEntryComponent extends React.Component<EntryProps> {
  onClickCancel(entry: scheduler_queue.ExecutorQueueEntry, e: React.MouseEvent<HTMLButtonElement>) {
    rpcService.service.cancelExecutions(invocation.CancelExecutionsRequest.create({
      invocationId: entry.invocationId,
      executionId: entry.taskId,
    }))
    e.stopPropagation()
    e.preventDefault()
  }

  render() {
    let start = this.props.entry.insertTime
    let isExecuting = false
    if (this.props.entry.stage != build.bazel.remote.execution.v2.ExecutionStage.Value.UNKNOWN) {
      start = this.props.entry.executionStartTime
      isExecuting = true
    }
    const duration = durationMillis((new Date().getTime() - timestampToDate(start).getTime()))
    let stageText = ""
    switch (this.props.entry.stage) {
      case Value.CACHE_CHECK:
        stageText = "Checking Action Cache"
        break
    }
    const stageDuration = durationMillis(new Date().getTime() - timestampToDate(this.props.entry.stageStartTime).getTime())
    if (stageText != "") {
      stageText += " (" + stageDuration + ")"
    }
    return (
        <Link
            href={`/invocation/${this.props.entry.invocationId}?actionDigest=${this.props.entry.actionDigest}#action`}>
          <div className={`card ${isExecuting ? "card-success" : "card-neutral"}`}>
            <div className="content">
              <div style={{display: "flex"}}>
                <div style={{display: "flex", flexDirection: "column", width: "1200px"}}>
                  <div>{this.props.entry.taskId}</div>
                  <div style={{display: "flex", marginTop: "24px"}}>
                    <div className="details">
                      <div className="executor-section" style={{display: "flex"}}>
                        <div style={{display: "flex"}}><div style={{fontWeight: 700}}>{!isExecuting ? "QUEUED" : "EXECUTING"}</div> ({duration})</div>
                        <div style={{marginLeft: "36px"}}>{stageText}</div>
                      </div>
                  </div>
                  </div>
                </div>
                <div>
                  <Button
                    onClick={this.onClickCancel.bind(this, this.props.entry)}
                    onMouseDown={e => e.stopPropagation()}
                    className="destructive"
                  >
                    Cancel
                  </Button>
                </div>
              </div>
            </div>
          </div>
        </Link>
    );
  }
}

interface DetailsProps {
  user: User;
  executorID: string;
}

interface DetailsState {
  entries: scheduler_queue.ExecutorQueueEntry[];
}

export class ExecutorDetailsComponent extends React.Component<DetailsProps, DetailsState> {
  state: DetailsState = {
    entries: [],
  }

  interval?: number;
  lastFetch: Date = new Date(0);
  fetching: boolean = false;

  componentDidMount() {
    this.interval = window.setInterval(() => this.fetch(), 100)
    this.fetch()
  }

  componentWillUnmount() {
    window.clearInterval(this.interval)
  }

  fetch() {
    if (this.fetching) {
      return
    }
    if ((new Date().getTime() - this.lastFetch.getTime()) < 100) {
      return
    }
    this.fetching = true
    const req = scheduler_queue.GetExecutionNodeDetailsRequest.create({executorId: this.props.executorID});
    rpcService.service.getExecutionNodeDetails(req).then((rsp) => {
      this.setState({entries: rsp.entries})
    }).catch((e) => {
      error_service.handleError(e)
    })
    this.fetching = false
    this.lastFetch = new Date()
  }

  render() {
    return (
        <>
        <div className="executors-page">
          <div className="shelf">
            <div className="container">
              <div className="breadcrumbs">
                {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
                <span>Executor {this.props.executorID}</span>
              </div>
              <div className="title">Executor queue for {this.props.executorID}</div>
            </div>
          </div>
          <div className="container">
            {this.state.entries.length == 0 && <div style={{marginTop: "16px"}}>There are no tasks in the queue.</div>}
            {this.state.entries.map(
                  (entry) => <ExecutorEntryComponent entry={entry}/>
              )}
          </div>
        </div>
        </>
    )
  }
}

type TabId = "status" | "setup";

interface Props {
  user: User;
  path: string;
}

interface State {
  userOwnedExecutorsSupported: boolean;
  regions: { name: string; response: scheduler.GetExecutionNodesResponse }[];
  executorKeys: api_key.IApiKey[];
  loading: FetchType[];
  schedulerUri: string;
  error: BuildBuddyError | null;
}

export default class ExecutorsComponent extends React.Component<Props, State> {
  state: State = {
    userOwnedExecutorsSupported: false,
    regions: [],
    executorKeys: [],
    loading: [],
    schedulerUri: "",
    error: null,
  };

  subscription?: Subscription;
  interval?: number;
  lastFetch: Date = new Date(0);
  fetching: boolean = false;

  componentWillMount() {
    document.title = `Executors | BuildBuddy`;
  }

  componentDidMount() {
    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
    this.interval = window.setInterval(() => this.fetch(), 100)
    this.fetch();
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
    window.clearInterval(this.interval!)
  }

  async fetchApiKeys() {
    if (!this.props.user) return;

    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.ApiKeys],
    }));

    try {
      const response = await rpcService.service.getApiKeys(
        api_key.GetApiKeysRequest.create({ groupId: this.props.user.selectedGroup.id })
      );
      const executorKeys = response.apiKey.filter((key) =>
        key.capability.some((cap) => cap == api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY)
      );
      this.setState({ executorKeys: executorKeys });
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      this.setState((prevState) => ({
        loading: [...prevState.loading].filter((f) => f != FetchType.ApiKeys),
      }));
    }
  }

  async fetchExecutors() {
    let updateLoading = false
    if (this.state.regions.length == 0) {
      updateLoading = true
      this.setState((prevState) => ({
        loading: [...prevState.loading, FetchType.Executors],
      }));
    }

    try {
      let regions = rpcService.regionalServices.size
        ? await Promise.all(
            Array.from(rpcService.regionalServices).map(([name, service]) =>
              service.getExecutionNodes(scheduler.GetExecutionNodesRequest.create({})).then((resp) => {
                return { name: name, response: resp };
              })
            )
          )
        : [
            await rpcService.service.getExecutionNodes(scheduler.GetExecutionNodesRequest.create({})).then((resp) => {
              return { name: "", response: resp };
            }),
          ];

      let userOwnedExecutorsSupported = regions.some((r) => r.response.userOwnedExecutorsSupported);
      this.setState({
        regions: regions,
        userOwnedExecutorsSupported: userOwnedExecutorsSupported,
      });
      if (userOwnedExecutorsSupported) {
        await this.fetchApiKeys();
        await this.fetchBazelConfig();
      }
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      if (updateLoading) {
        this.setState((prevState) => ({
          loading: [...prevState.loading].filter((f) => f != FetchType.Executors),
        }));
      }
    }
  }

  async fetchBazelConfig() {
    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.BazelConfig],
    }));

    try {
      let request = new bazel_config.GetBazelConfigRequest();
      request.host = window.location.host;
      request.protocol = window.location.protocol;
      const response = await rpcService.service.getBazelConfig(request);
      const schedulerUri = response.configOption.find((opt) => opt.flagName == "remote_executor");
      this.setState({ schedulerUri: schedulerUri?.flagValue || "" });
    } catch (e) {
      this.setState({ error: BuildBuddyError.parse(e) });
    } finally {
      this.setState((prevState) => ({
        loading: [...prevState.loading].filter((f) => f != FetchType.BazelConfig),
      }));
    }
  }

  fetch() {
    if (this.fetching) {
      return
    }
    if ((new Date().getTime() - this.lastFetch.getTime()) < 100) {
      return
    }
    this.fetching = true
    this.fetchExecutors()
    this.fetching = false
    this.lastFetch = new Date()
  }

  onClickTab(tabId: TabId) {
    router.navigateTo(`/executors/${tabId}`);
  }

  // "bring your own runners" is enabled for the installation (i.e. BuildBuddy Cloud deployment).
  renderWithGroupOwnedExecutorsEnabled() {
    const allNodes = this.state.regions.flatMap((r) => r.response.executor);
    const defaultTabId = allNodes.length > 0 ? "status" : "setup";
    const activeTab = (this.props.path.substring("/executors/".length) || defaultTabId) as TabId;

    return (
      <>
        <div className="tabs">
          <div
            className={`tab ${activeTab === "status" ? "selected" : ""}`}
            onClick={this.onClickTab.bind(this, "status")}>
            Status
          </div>
          <div
            className={`tab ${activeTab === "setup" ? "selected" : ""}`}
            onClick={this.onClickTab.bind(this, "setup")}>
            Setup
          </div>
        </div>
        {activeTab === "status" && (
          <>
            {allNodes.some((node) => !node.isDefault) && (
              <Banner type="warning">
                <div>
                  Self-hosted executors are not the default for this organization. To change this, enable "Default to
                  self-hosted executors" in your organization settings.
                </div>
                <div>
                  <LinkButton href="/settings/" className="organization-settings-button">
                    Open settings
                  </LinkButton>
                </div>
              </Banner>
            )}
            <ExecutorsList regions={this.state.regions} />
            {!allNodes.length && this.props.user.selectedGroup.useGroupOwnedExecutors && (
              <div className="empty-state">
                <h1>No self-hosted executors are connected.</h1>
                <p>Click the "setup" tab to deploy your executors.</p>
              </div>
            )}
            {!allNodes.length && !this.props.user.selectedGroup.useGroupOwnedExecutors && (
              <div className="empty-state">
                <h1>You're currently using BuildBuddy cloud executors.</h1>
                <p>Click the "setup" tab for instructions on self-hosting executors.</p>
              </div>
            )}
          </>
        )}
        {activeTab === "setup" && (
          <ExecutorSetup
            user={this.props.user}
            executorKeys={this.state.executorKeys}
            executors={allNodes}
            schedulerUri={this.state.schedulerUri}
          />
        )}
      </>
    );
  }

  // "bring your own runners" is not enabled for the installation (i.e. onprem deployment)
  renderWithoutGroupOwnedExecutorsEnabled() {
    const allNodes = this.state.regions.flatMap((r) => r.response.executor);
    if (allNodes.length == 0) {
      return (
        <div className="empty-state">
          <h1>No remote execution nodes found!</h1>
          <p>
            Check out our documentation on deploying remote executors:
            <br />
            <br />
            <a className="button" href="https://docs.buildbuddy.io/docs/enterprise-rbe">
              Click here for more information
            </a>
          </p>
        </div>
      );
    } else {
      return <ExecutorsList regions={this.state.regions} />;
    }
  }

  render() {
    return (
      <div className="executors-page">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
              <span>Executors</span>
            </div>
            <div className="title">Executors</div>
          </div>
        </div>
        {this.state.error && <div className="error-message">{this.state.error.message}</div>}
        {this.state.loading.length > 0 && <div className="loading"></div>}
        {this.state.loading.length == 0 && this.state.error == null && (
          <div className="container">
            {this.state.userOwnedExecutorsSupported && this.renderWithGroupOwnedExecutorsEnabled()}
            {!this.state.userOwnedExecutorsSupported && this.renderWithoutGroupOwnedExecutorsEnabled()}
          </div>
        )}
      </div>
    );
  }
}
