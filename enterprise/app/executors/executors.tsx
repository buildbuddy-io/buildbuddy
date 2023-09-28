import React from "react";
import Banner from "../../../app/components/banner/banner";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { User } from "../../../app/auth/auth_service";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import ExecutorCardComponent from "./executor_card";
import { Subscription } from "rxjs";
import { api_key } from "../../../proto/api_key_ts_proto";
import { bazel_config } from "../../../proto/bazel_config_ts_proto";
import router from "../../../app/router/router";
import Select, { Option } from "../../../app/components/select/select";
import LinkButton from "../../../app/components/button/link_button";
import { Cpu, Globe, Hash, Laptop, LucideIcon } from "lucide-react";

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

class ExecutorsList extends React.Component<ExecutorsListProps> {
  render() {
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
                        <ExecutorCardComponent node={node.executor.node} isDefault={node.executor.isDefault} />
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

  componentWillMount() {
    document.title = `Executors | BuildBuddy`;
  }

  componentDidMount() {
    this.fetch();
    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetch(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
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
    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.Executors],
    }));

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
      this.setState((prevState) => ({
        loading: [...prevState.loading].filter((f) => f != FetchType.Executors),
      }));
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
    this.fetchExecutors();
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
