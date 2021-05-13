import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { User } from "../../../app/auth/auth_service";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import ExecutorCardComponent from "./executor_card";
import { Subscription } from "rxjs";
import capabilities from "../../../app/capabilities/capabilities";
import { api_key } from "../../../proto/api_key_ts_proto";
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
        <p>The executor is available as a Docker image that you can deploy using your preferred mechanism.</p>
        <p>Example running an executor manually using the Docker CLI is shown below.</p>
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
          docker run gcr.io/flame-public/buildbuddy-executor-enterprise:latest \ <br />
          &emsp;--executor.app_target {this.props.schedulerUri} \ <br />
          &emsp;--executor.enable_work_streaming true \ <br />
          &emsp;--executor.api_key {this.props.executorKeys[this.state.selectedExecutorKeyIdx]?.value} <br />
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
        <h3>1. Create API key for executor registration</h3>
        {this.props.executorKeys.length == 0 && (
          <div>
            <p>There are no API keys with executor capability configured for your organization.</p>
            <p>API keys are used to authorize executors that wish to register to process work.</p>
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
            <h3>2. Deploy executors</h3>
            <ExecutorDeploy executorKeys={this.props.executorKeys} schedulerUri={this.props.schedulerUri} />
            {this.props.executors.length == 1 && <p>There is 1 connected executor.</p>}
            {this.props.executors.length != 1 && <p>There are {this.props.executors.length} connected executors.</p>}
            <h3>3. Switch to self-hosted executors in organization settings</h3>
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
    return (
      <>
        {this.props.executors.length == 1 && <p>There is 1 connected executor.</p>}
        {this.props.executors.length > 1 && <p>There are {this.props.executors.length} connected executors.</p>}
        {this.props.executors.length < 3 && (
          <p>For best performance and reliability we suggest running a minimum of 3 executors.</p>
        )}
        <div className="executor-cards">
          {this.props.executors.map((node) => (
            <ExecutorCardComponent node={node} />
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
  nodes: scheduler.IExecutionNode[];
  executorKeys: api_key.IApiKey[];
  loading: FetchType[];
  schedulerUri: string;
  error: BuildBuddyError | null;
}

export default class ExecutorsComponent extends React.Component<Props, State> {
  state: State = {
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
      this.setState({ nodes: response.executionNode });
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
      const response = await rpcService.service.getBazelConfig({});
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
    if (capabilities.userOwnedExecutors) {
      this.fetchApiKeys();
      this.fetchBazelConfig();
    }
  }

  // "bring your own runners" is enabled for the installation (i.e. BuildBuddy Cloud deployment).
  renderWithGroupOwnedExecutorsEnabled() {
    if (this.props.user?.selectedGroup?.useGroupOwnedExecutors) {
      if (this.state.nodes.length == 0) {
        return (
          <div className="empty-state">
            <h2>No self-hosted executors connected.</h2>
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
          <ExecutorsList executors={this.state.nodes} />
          <h2>Deploying additional executors</h2>
          <ExecutorDeploy executorKeys={this.state.executorKeys} schedulerUri={this.state.schedulerUri} />
        </>
      );
    } else {
      return (
        <div className="empty-state">
          <h2>You're all set to use BuildBuddy Cloud executors.</h2>
          <p>See the instructions below if you'd like to use self-hosted executors.</p>
          <h2>Using self-hosted executors</h2>
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
          <h2>No remote execution nodes found!</h2>
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
            <div className="title">Executors</div>
          </div>
        </div>
        {this.state.error && <div className="error-message">{this.state.error.message}</div>}
        {this.state.loading.length > 0 && <div className="loading"></div>}
        {this.state.loading.length == 0 && this.state.error == null && (
          <div className="container narrow">
            {capabilities.userOwnedExecutors && this.renderWithGroupOwnedExecutorsEnabled()}
            {!capabilities.userOwnedExecutors && this.renderWithoutGroupOwnedExecutorsEnabled()}
          </div>
        )}
      </div>
    );
  }
}
