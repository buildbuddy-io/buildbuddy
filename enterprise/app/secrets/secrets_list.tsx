import React from "react";
import rpc_service from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import { secrets } from "../../../proto/secrets_ts_proto";
import { TextLink } from "../../../app/components/link/link";
import LinkButton, { OutlinedLinkButton } from "../../../app/components/button/link_button";
import { Lock } from "lucide-react";
import { OutlinedButton } from "../../../app/components/button/button";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import alert_service from "../../../app/alert/alert_service";

interface State {
  loading?: boolean;
  response?: secrets.IListSecretsResponse;

  secretToDelete?: string;
  deleteLoading?: boolean;
}

export default class SecretsListComponent extends React.Component<{}, State> {
  state: State = {};

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
    this.setState({ loading: true });
    rpc_service.service
      .listSecrets({})
      .then((response) => this.setState({ response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onClickDelete(name: string) {
    this.setState({ secretToDelete: name });
  }
  private onCloseDeleteModal() {
    this.setState({ secretToDelete: undefined });
  }
  private onConfirmDelete() {
    this.setState({ deleteLoading: true });
    rpc_service.service
      .deleteSecret({ secret: { name: this.state.secretToDelete } })
      .then(() => {
        alert_service.success("Secret deleted successfully.");
        this.setState({ secretToDelete: undefined });
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ deleteLoading: false }));
  }

  render() {
    return (
      <div className="secrets-list settings-content">
        <div className="settings-option-title">Secrets</div>
        <div className="settings-option-description">
          Secrets let you securely pass sensitive data to actions run with remote execution.{" "}
          <TextLink href="https://buildbuddy.io/docs/secrets">Learn more</TextLink>
        </div>
        <div>
          <LinkButton className="big-button" href="/settings/org/secrets/new">
            Create new secret
          </LinkButton>
        </div>
        {this.state.loading ? (
          <div className="loading" />
        ) : (
          <div className="secrets-table">
            {this.state.response?.secret.map((secret) => (
              <div className="secrets-row">
                <Lock className="icon lock-icon" />
                <span className="secret-name code-font">{secret.name}</span>
                <OutlinedLinkButton
                  className="edit-button"
                  href={`/settings/org/secrets/edit?name=${encodeURIComponent(secret.name)}`}>
                  Edit
                </OutlinedLinkButton>
                <OutlinedButton
                  className="delete-button destructive"
                  onClick={this.onClickDelete.bind(this, secret.name)}>
                  Delete
                </OutlinedButton>
              </div>
            ))}
          </div>
        )}
        <SimpleModalDialog
          title="Confirm deletion"
          isOpen={Boolean(this.state.secretToDelete)}
          submitLabel="Delete"
          onRequestClose={this.onCloseDeleteModal.bind(this)}
          onSubmit={this.onConfirmDelete.bind(this)}
          loading={this.state.deleteLoading}
          className="delete-secret-dialog"
          destructive>
          Delete <span className="secret-name code-font">{this.state.secretToDelete}</span>? This cannot be undone.
        </SimpleModalDialog>
      </div>
    );
  }
}
