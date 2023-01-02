import React from "react";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import auth_service from "../../../app/auth/auth_service";
import rpc_service from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import TextInput from "../../../app/components/input/input";

interface State {
  query: string;

  visible: boolean;
  loading: boolean;
}

export default class GroupSearchComponent extends React.Component<{}, State> {
  state: State = { query: "", visible: false, loading: false };

  componentDidMount() {
    window.addEventListener("groupSearchClick", () => this.onClickGroupSearch());
  }

  private onClickGroupSearch() {
    this.setState({ visible: true, query: "" });
  }

  private onClose() {
    this.setState({ visible: false });
  }

  private onChangeQuery(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ query: e.target.value });
  }

  private onSearch() {
    const query = this.state.query.trim();

    // If the query looks like a group ID, navigate directly to it.
    if (query.startsWith("GR")) {
      const groupId = query;
      auth_service.enterImpersonationMode(groupId);
      return;
    }

    // Otherwise try to look up the group by its exact URL identifier.
    this.setState({ loading: true });
    rpc_service.service
      .getGroup({ urlIdentifier: query })
      .then((response) => auth_service.enterImpersonationMode(response.id))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  render() {
    return (
      <SimpleModalDialog
        title="Go to org"
        submitLabel="Search"
        isOpen={this.state.visible}
        onRequestClose={this.onClose.bind(this)}
        onSubmit={this.onSearch.bind(this)}
        loading={this.state.loading}>
        <TextInput
          value={this.state.query}
          onChange={this.onChangeQuery.bind(this)}
          placeholder="Group ID ('GR1234...') or URL identifier ('acme-inc')"
          style={{ width: "100%" }}
          autoFocus
        />
      </SimpleModalDialog>
    );
  }
}
