import React from "react";
import alert_service from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/auth_service";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import TextInput from "../../../app/components/input/input";
import Modal from "../../../app/components/modal/modal";
import Spinner from "../../../app/components/spinner/spinner";
import rpc_service from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";

interface SSOConfigProps {
  user: User;
}

interface SSOConfigState {
  loading: boolean;
  loadError?: string;
  currentUrl: string;
  inputUrl: string;
  isConfirmOpen: boolean;
  submitting: boolean;
}

export default class SSOConfigComponent extends React.Component<SSOConfigProps, SSOConfigState> {
  state: SSOConfigState = {
    loading: true,
    currentUrl: "",
    inputUrl: "",
    isConfirmOpen: false,
    submitting: false,
  };

  componentDidMount() {
    this.fetchConfig();
  }

  private async fetchConfig() {
    this.setState({ loading: true, loadError: undefined });
    try {
      const rsp = await rpc_service.service.getSSOConfig(grp.GetSSOConfigRequest.create({}));
      const url = rsp.config?.samlIdpMetadataUrl ?? "";
      this.setState({ loading: false, currentUrl: url, inputUrl: url });
    } catch (e) {
      this.setState({ loading: false, loadError: String(e) });
    }
  }

  private handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({ inputUrl: e.target.value });
  };

  private openConfirm = (e: React.FormEvent) => {
    e.preventDefault();
    if (this.state.inputUrl.trim() === this.state.currentUrl.trim()) {
      return;
    }
    this.setState({ isConfirmOpen: true });
  };

  private closeConfirm = () => {
    if (this.state.submitting) return;
    this.setState({ isConfirmOpen: false });
  };

  private confirmSave = async () => {
    const newUrl = this.state.inputUrl.trim();
    this.setState({ submitting: true });
    try {
      await rpc_service.service.setSSOConfig(
        grp.SetSSOConfigRequest.create({
          config: grp.SSOConfig.create({ samlIdpMetadataUrl: newUrl }),
        })
      );
      alert_service.success("SSO configuration updated");
      this.setState({
        currentUrl: newUrl,
        inputUrl: newUrl,
        isConfirmOpen: false,
        submitting: false,
      });
    } catch (e) {
      alert_service.error("Failed to update SSO configuration: " + e);
      this.setState({ submitting: false, isConfirmOpen: false });
    }
  };

  render() {
    const { loading, loadError, currentUrl, inputUrl, isConfirmOpen, submitting } = this.state;
    const dirty = inputUrl.trim() !== currentUrl.trim();

    return (
      <>
        <div className="settings-option-title">SSO configuration</div>
        <div className="settings-option-description">
          Configure SAML SSO by providing the IdP metadata URL. Leave blank to disable SSO.
        </div>
        {loading ? (
          <Spinner />
        ) : loadError ? (
          <div className="error-text">Failed to load SSO configuration: {loadError}</div>
        ) : (
          <form onSubmit={this.openConfirm} className="sso-config-form" style={{ marginTop: "16px" }}>
            <TextInput
              style={{ width: "100%", maxWidth: "640px" }}
              placeholder="https://idp.example.com/metadata"
              value={inputUrl}
              onChange={this.handleInputChange}
            />
            <div style={{ marginTop: "12px" }}>
              <FilledButton type="submit" disabled={!dirty || submitting}>
                Save
              </FilledButton>
            </div>
          </form>
        )}
        <Modal isOpen={isConfirmOpen} onRequestClose={this.closeConfirm}>
          <Dialog className="sso-config-dialog">
            <DialogHeader>
              <DialogTitle>Confirm SSO configuration change</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>Change the SAML IdP metadata URL for this organization?</p>
              <p>
                <b>From:</b> {currentUrl || <i>(unset)</i>}
              </p>
              <p>
                <b>To:</b> {inputUrl.trim() || <i>(unset)</i>}
              </p>
              <p>The URL will be fetched and validated before being saved.</p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <OutlinedButton onClick={this.closeConfirm} disabled={submitting}>
                  Cancel
                </OutlinedButton>
                <FilledButton onClick={this.confirmSave} disabled={submitting}>
                  {submitting ? "Saving..." : "Confirm"}
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
