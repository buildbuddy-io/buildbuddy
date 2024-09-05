import React from "react";
import { User } from "../../../app/auth/user";
import router from "../../../app/router/router";
import auth_service from "../../../app/auth/auth_service";
import Select, { Option } from "../../../app/components/select/select";
import Button from "../../../app/components/button/button";
import { grp } from "../../../proto/group_ts_proto";
import rpc_service, { Cancelable } from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import { CheckCircle, TerminalIcon, XCircle } from "lucide-react";

export interface CliLoginProps {
  user: User;
  search: URLSearchParams;
}

interface State {
  loadingGroup: boolean;
  creatingApiKey: boolean;
  group?: grp.IGroup;
  selectedGroupIdOption: string;
}

export default class CliLoginComponent extends React.Component<CliLoginProps, State> {
  state: State = {
    loadingGroup: false,
    creatingApiKey: false,
    selectedGroupIdOption: "",
  };

  private currentRPC?: Cancelable;

  componentDidMount() {
    document.title = "CLI Login | BuildBuddy";
    this.update();
  }

  componentDidUpdate() {
    this.update();
  }

  async update() {
    // If this is the "completed" page then nothing needs to be fetched.
    if (this.props.search.get("complete")) return;

    // If we're creating a personal API key, let that finish.
    if (this.state.creatingApiKey) return;

    // Validate that we have the expected URL params from the CLI.
    for (const expectedParam of ["cli_url", "workspace"]) {
      if (!this.props.search.has(expectedParam)) {
        // Fall back to the old login flow in settings, where the user can
        // manually pick an API key.
        error_service.handleError(`invalid CLI login URL (missing "${expectedParam}")`);
        router.replaceURL("/settings/cli-login");
        return;
      }
    }

    // cli_url must be a localhost URL.
    const cliURL = this.props.search.get("cli_url") ?? "";
    if (!isLocalhostURL(cliURL)) {
      console.log("cli_url", this.props.search.get("cli_url"));
      error_service.handleError(`invalid CLI login URL ("cli_url" must be a localhost URL)`);
      router.replaceURL("/settings/cli-login");
      return;
    }

    let group: grp.IGroup | null = null;
    let slug = this.props.search.get("org");
    let id = this.props.search.get("group_id");
    if (id) {
      group = this.props.user.groups.find((g) => g.id === id) ?? null;
    } else if (slug) {
      group = this.props.user.groups.find((g) => g.urlIdentifier === slug) ?? null;
      if (!group) {
        group = await this.fetchGroupInfo(slug);
      }
    } else if (this.props.user.groups.length === 1) {
      group = this.props.user.groups[0];
    }

    // If no group is pre-selected in the URL, show the group picker. Note
    // that the router guarantees that the user is a member of at least group.
    if (!group) return;

    // If the user doesn't have access to the specified org, redirect them to a
    // page where they can request to join the org if it exists.
    const isGroupMember = this.props.user.groups.some((g) => g.id === group!.id);
    if (!isGroupMember) {
      // TODO: support group ID in case url_identifier is empty/missing?
      router.replaceURL(`/org/join/${group.urlIdentifier}`);
      return;
    }

    // If necessary, redirect so that the org is selected. This will allow us to
    // make the necessary API key RPCs to finalize CLI login.
    if (group.id !== this.props.user.selectedGroup.id) {
      console.debug("Setting selected group ID");
      auth_service.setSelectedGroupId(group.id ?? "", group.url ?? "");
      return;
    }

    // At this point, `group` is the same group as the user's selected group.
    // Use `selectedGroup` going forward, since it has the complete group proto.
    group = this.props.user.selectedGroup;

    // If the org has personal API keys enabled, create a new key for this login
    // attempt and redirect.
    if (group.userOwnedKeysEnabled) {
      this.createPersonalKeyAndRedirect();
      return;
    }

    // Otherwise, fall back to the old /settings/cli-login flow, instructing the
    // user to manually select an API key.
    router.replaceURL("/settings/cli-login");
  }

  async createPersonalKeyAndRedirect() {
    console.log("Creating personal API key");
    this.setState({ creatingApiKey: true });
    return rpc_service.service
      .createUserApiKey({
        label: `CLI key - ${this.props.search.get("workspace")}`,
        capability: this.props.user.selectedGroup.allowedUserApiKeyCapabilities,
      })
      .then((response) => {
        if (!response.apiKey) {
          throw new Error("server error: missing API key in response");
        }
        const redirectURL = new URL(this.props.search.get("cli_url")!);
        redirectURL.searchParams.set("token", response.apiKey.value);
        window.location.href = redirectURL.toString();
      })
      .catch((e) => error_service.handleError(e));
  }

  async fetchGroupInfo(slug: string) {
    this.currentRPC?.cancel();
    this.setState({ loadingGroup: true });
    this.currentRPC = rpc_service.service
      .getGroup({ urlIdentifier: slug })
      .then(
        (response) =>
          new grp.Group({
            id: response.id,
            url: response.url,
            urlIdentifier: slug,
          })
      )
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loadingGroup: false }));
    return this.currentRPC;
  }

  onChangeSelectedGroup(e: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ selectedGroupIdOption: e.target.value });
  }

  onClickComplete() {
    router.replaceParams({
      ...Object.fromEntries(this.props.search.entries()),
      group_id: this.state.selectedGroupIdOption || this.props.user.selectedGroup.id,
    });
  }

  render() {
    if (this.props.search.get("complete")) {
      const failed = !!this.props.search.get("cliLoginError");
      return (
        <div className="cli-login" debug-id="cli-login-complete">
          <div className="card">
            {failed ? <XCircle className="icon red" /> : <CheckCircle className="icon green" />}
            <div className="content">
              <div className="title">{failed ? "CLI login failed" : "CLI login succeeded"}</div>
              <div className="details">
                <div className="explanation">
                  {failed ? "BuildBuddy login failed - see terminal." : "BuildBuddy login complete - see terminal."}
                </div>
              </div>
            </div>
          </div>
        </div>
      );
    }

    if (this.state.loadingGroup) {
      return <div className="loading" />;
    }

    if (this.state.group) {
      return <div>Redirecting...</div>;
    }

    return (
      <div className="cli-login">
        <div className="card">
          <TerminalIcon className="icon" />
          <div className="content">
            <div className="title">Complete CLI login</div>
            <div className="details">
              <div className="explanation">
                Select which organization to authorize for the workspace{" "}
                <span className="inline-code">{this.props.search.get("workspace")}</span>.
              </div>
              <div className="controls">
                <Select
                  value={this.state.selectedGroupIdOption || this.props.user.selectedGroup.id}
                  onChange={this.onChangeSelectedGroup.bind(this)}
                  debug-id="org">
                  {this.props.user.groups.map((group) => (
                    <Option key={group.id} value={group.id}>
                      {group.name}
                    </Option>
                  ))}
                </Select>
                <Button onClick={this.onClickComplete.bind(this)} debug-id="authorize">
                  Authorize
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

function isLocalhostURL(url: string): boolean {
  try {
    // TODO: use URL.canParse once we upgrade webdriver chromium version.
    return new URL(url).hostname === "localhost";
  } catch {
    return false;
  }
}
