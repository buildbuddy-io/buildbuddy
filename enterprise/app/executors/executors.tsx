import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { User } from "../../../app/auth/auth_service";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import ExecutorCardComponent from "./executor_card";
import { Subscription } from "rxjs";
import capabilities from "../../../app/capabilities/capabilities";
import { api_key } from "../../../proto/api_key_ts_proto";
import { bazel_config } from "../../../proto/bazel_config_ts_proto";
import { FilledButton } from "../../../app/components/button/button";
import router from "../../../app/router/router";
import Select, { Option } from "../../../app/components/select/select";

enum FetchType {
  Executors,
  ApiKeys,
  BazelConfig,
}

function onClickLink(href: string, e: React.MouseEvent<HTMLAnchorElement>) {
  e.preventDefault();
  router.navigateTo(href);
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
        {/* enable_work_streaming is temporary and will be removed before launch. 
            presence of api_key will implicitely enable work streaming. */}
        <code>
          <pre>
            {`docker run --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \\
    gcr.io/flame-public/buildbuddy-executor-enterprise:latest \\
    --executor.docker_socket=/var/run/docker.sock \\
    --executor.app_target=${this.props.schedulerUri} \\
    --executor.api_key=${this.props.executorKeys[this.state.selectedExecutorKeyIdx]?.value}`}
          </pre>
        </code>
      </>
    );
  }
}

interface ExecutorSetupProps {
  executorKeys: api_key.IApiKey[];
  executors: scheduler.IExecutionNode[];
  schedulerUri: string;
}

class ExecutorSetup extends React.Component<ExecutorSetupProps> {
  state = { selectedExecutorKeyIdx: 0 };

  render() {
    return (
      <>
        <hr />
        <h2>1. Create an API key for executor registration</h2>
        {this.props.executorKeys.length == 0 && (
          <div>
            <p>There are no API keys with the executor capability configured for your organization.</p>
            <p>API keys are used to authorize self-hosted executors.</p>
            <FilledButton className="manage-keys-button">
              <a href="/settings/" onClick={onClickLink.bind("/settings")}>
                Manage keys
              </a>
            </FilledButton>
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
            {this.props.executors.length == 1 && <p>You have 1 self-hosted executor connected.</p>}
            {this.props.executors.length != 1 && (
              <p>You have {this.props.executors.length} self-hosted executors connected.</p>
            )}
            <h2>3. Switch to self-hosted executors in organization settings</h2>
            <p>Enable "Use self-hosted executors" on the Organization Settings page.</p>
            <FilledButton className="organization-settings-button">
              <a href="/settings/" onClick={onClickLink.bind("/settings")}>
                Open settings
              </a>
            </FilledButton>
          </>
        )}
      </>
    );
  }
}

interface ExecutorsListProps {
  executors: scheduler.IExecutionNode[];
}

class ExecutorsList extends React.Component<ExecutorsListProps> {
  render() {
    let executorsByPool = new Map<string, scheduler.IExecutionNode[]>();
    for (const e of this.props.executors) {
      const key = e.os + "-" + e.arch + "-" + e.pool;
      if (!executorsByPool.has(key)) {
        executorsByPool.set(key, []);
      }
      executorsByPool.get(key).push(e);
    }
    const keys = Array.from(executorsByPool.keys()).sort();

    return (
      <>
        <div className="executor-cards">
          {}
          {keys
            .map((key) => executorsByPool.get(key))
            .map((executors) => (
              <>
                <h2>
                  {executors[0].os}/{executors[0].arch} {executors[0].pool || "Default Pool"}
                </h2>
                {executors.length == 1 && <p>There is 1 self-hosted executor in this pool.</p>}
                {executors.length > 1 && <p>There are {executors.length} self-hosted executors in this pool.</p>}
                {executors.length < 3 && (
                  <p>For better performance and reliability, we suggest running a minimum of 3 executors per pool.</p>
                )}
                {executors.map((node) => (
                  <ExecutorCardComponent node={node} />
                ))}
              </>
            ))}
        </div>
      </>
    );
  }
}

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  userOwnedExecutorsSupported: boolean;
  nodes: scheduler.IExecutionNode[];
  executorKeys: api_key.IApiKey[];
  loading: FetchType[];
  schedulerUri: string;
  error: BuildBuddyError | null;
}

export default class ExecutorsComponent extends React.Component<Props, State> {
  state: State = {
    userOwnedExecutorsSupported: false,
    nodes: [],
    executorKeys: [],
    loading: [],
    schedulerUri: "",
    error: null,
  };

  subscription: Subscription;

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

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash || this.props.search != prevProps.search) {
      this.fetch();
    }
  }

  async fetchApiKeys() {
    if (!this.props.user) return;

    this.setState((prevState) => ({
      loading: [...prevState.loading, FetchType.ApiKeys],
    }));

    try {
      const response = await rpcService.service.getApiKeys({ groupId: this.props.user.selectedGroup.id });
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
      const response = await rpcService.service.getExecutionNodes({});
      this.setState({
        nodes: response.executionNode,
        userOwnedExecutorsSupported: response.userOwnedExecutorsSupported,
      });
      if (response.userOwnedExecutorsSupported) {
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
      this.setState({ schedulerUri: schedulerUri?.flagValue });
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

  // "bring your own runners" is enabled for the installation (i.e. BuildBuddy Cloud deployment).
  renderWithGroupOwnedExecutorsEnabled() {
    if (this.props.user?.selectedGroup?.useGroupOwnedExecutors) {
      if (this.state.nodes.length == 0) {
        return (
          <div className="empty-state">
            <h1>No self-hosted executors connected.</h1>
            <p>Self-hosted executors are enabled, but there are no executors connected.</p>
            <p>Follow the instructions below to deploy your executors.</p>
            <ExecutorSetup
              executorKeys={this.state.executorKeys}
              executors={this.state.nodes}
              schedulerUri={this.state.schedulerUri}
            />
          </div>
        );
      }
      return (
        <>
          <h1>Self-hosted executors</h1>
          <ExecutorsList executors={this.state.nodes} />
          <hr />
          <h1>Deploying additional executors</h1>
          <ExecutorDeploy executorKeys={this.state.executorKeys} schedulerUri={this.state.schedulerUri} />
        </>
      );
    } else {
      return (
        <div className="empty-state">
          <h1>You're currently using BuildBuddy Cloud executors.</h1>
          <p>See the instructions below if you'd like to use self-hosted executors.</p>
          <ExecutorSetup
            executorKeys={this.state.executorKeys}
            executors={this.state.nodes}
            schedulerUri={this.state.schedulerUri}
          />
        </div>
      );
    }
  }

  // "bring your own runners" is not enabled for the installation (i.e. onprem deployment)
  renderWithoutGroupOwnedExecutorsEnabled() {
    if (this.state.nodes.length == 0) {
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
      return <ExecutorsList executors={this.state.nodes} />;
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
