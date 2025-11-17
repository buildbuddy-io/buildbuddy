import React from "react";
import auth_service from "../../../app/auth/auth_service";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import TextInput from "../../../app/components/input/input";
import error_service from "../../../app/errors/error_service";

interface State {
  query: string;

  visible: boolean;
  loading: boolean;
}

export default class GroupSearchComponent extends React.Component<{}, State> {
  state: State = { query: "", visible: false, loading: false };

  componentDidMount(): void {
    window.addEventListener("groupSearchClick", () => this.onClickGroupSearch());
  }

  private onClickGroupSearch(): void {
    this.setState({ visible: true, query: "" });
  }

  private onClose(): void {
    this.setState({ visible: false });
  }

  private onChangeQuery(e: React.ChangeEvent<HTMLInputElement>): void {
    this.setState({ query: e.target.value });
  }

  private onSearch(): void {
    const query = this.state.query.trim();
    this.setState({ loading: true });
    auth_service
      .enterImpersonationMode(query)
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  render(): JSX.Element {
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
