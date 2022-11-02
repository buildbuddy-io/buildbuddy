import React from "react";
import error_service from "../../../app/errors/error_service";
import rpc_service from "../../../app/service/rpc_service";
import { quota } from "../../../proto/quota_ts_proto";
import LinkButton, { OutlinedLinkButton } from "../../../app/components/button/link_button";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import Spinner from "../../../app/components/spinner/spinner";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import alert_service from "../../../app/alert/alert_service";

type State = {
  loading?: boolean;
  response?: quota.GetNamespaceResponse;

  namespaceToDelete?: string;
  deleteLoading?: boolean;
};

export default class NamespacesComponent extends React.Component<{}, State> {
  state: State = {};

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
    this.setState({ loading: true });
    rpc_service.service
      .getNamespace({})
      .then((response) => this.setState({ response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onClickDelete(namespace: string) {
    this.setState({ namespaceToDelete: namespace });
  }
  private onRequestCloseDialog() {
    this.setState({ namespaceToDelete: "" });
  }
  private onConfirmDelete() {
    const namespace = this.state.namespaceToDelete;
    this.setState({ deleteLoading: true });
    rpc_service.service
      .removeNamespace({ namespace })
      .then(() => {
        this.setState({ namespaceToDelete: "" });
        alert_service.success(`Namespace "${namespace}" deleted successfully.`);
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ deleteLoading: false }));
  }

  render() {
    if (this.state.loading) return <div className="loading" />;
    if (!this.state.response) return null;

    return (
      <div className="quota-namespaces quota-column-layout">
        <div className="settings-option-title">Quota namespaces</div>
        <div className="ns-controls">
          <LinkButton href="/settings/server/quota/bucket" className="big-button">
            Create new namespace
          </LinkButton>
        </div>
        {this.state.response.namespaces.length ? (
          <div className="ns-list">
            {this.state.response.namespaces.map((namespace) => (
              <div className="ns-item">
                <span className="name">{namespace.name}</span>
                <OutlinedLinkButton
                  key={namespace.name}
                  href={`/settings/server/quota/namespace?name=${encodeURIComponent(namespace.name)}`}>
                  Edit
                </OutlinedLinkButton>
                <OutlinedButton className="destructive" onClick={this.onClickDelete.bind(this, namespace.name)}>
                  Delete
                </OutlinedButton>
              </div>
            ))}
          </div>
        ) : (
          <div>No namespaces found. Create one to get started.</div>
        )}
        <SimpleModalDialog
          title="Confirm deletion"
          isOpen={Boolean(this.state.namespaceToDelete)}
          submitLabel="Delete"
          onRequestClose={this.onRequestCloseDialog.bind(this)}
          onSubmit={this.onConfirmDelete.bind(this)}
          loading={this.state.deleteLoading}
          destructive>
          This will delete all buckets under <b>{this.state.namespaceToDelete}</b>, including any users assigned to the
          buckets.
        </SimpleModalDialog>
      </div>
    );
  }
}
