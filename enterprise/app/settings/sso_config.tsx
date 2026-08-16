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
import { TextLink } from "../../../app/components/link/link";
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
  // The persisted metadata URL. Empty means SSO is disabled.
  currentUrl: string;

  // Enable / change form.
  isEditing: boolean;
  inputUrl: string;
  isSaving: boolean;

  // Disable confirmation dialog.
  isDisableModalOpen: boolean;
  isDisabling: boolean;
}

export default class SSOConfigComponent extends React.Component<SSOConfigProps, SSOConfigState> {
  state: SSOConfigState = {
    loading: true,
    currentUrl: "",
    isEditing: false,
    inputUrl: "",
    isSaving: false,
    isDisableModalOpen: false,
    isDisabling: false,
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

  private async setMetadataUrl(url: string) {
    await rpc_service.service.setSSOConfig(
      grp.SetSSOConfigRequest.create({
        config: grp.SSOConfig.create({ samlIdpMetadataUrl: url }),
      })
    );
  }

  private handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({ inputUrl: e.target.value });
  };

  private onClickChange = () => {
    this.setState({ isEditing: true, inputUrl: this.state.currentUrl });
  };

  private onCancelEdit = () => {
    this.setState({ isEditing: false, inputUrl: this.state.currentUrl });
  };

  private onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const newUrl = this.state.inputUrl.trim();
    if (!newUrl || newUrl === this.state.currentUrl.trim()) {
      return;
    }
    this.setState({ isSaving: true });
    try {
      await this.setMetadataUrl(newUrl);
      alert_service.success("SSO configuration updated");
      this.setState({ currentUrl: newUrl, inputUrl: newUrl, isEditing: false, isSaving: false });
    } catch (e) {
      alert_service.error("Failed to update SSO configuration: " + e);
      this.setState({ isSaving: false });
    }
  };

  private onClickDisable = () => {
    this.setState({ isDisableModalOpen: true });
  };

  private onCloseDisableModal = () => {
    if (this.state.isDisabling) return;
    this.setState({ isDisableModalOpen: false });
  };

  private onConfirmDisable = async () => {
    this.setState({ isDisabling: true });
    try {
      await this.setMetadataUrl("");
      alert_service.success("SSO disabled");
      this.setState({ currentUrl: "", inputUrl: "", isEditing: false, isDisableModalOpen: false, isDisabling: false });
    } catch (e) {
      alert_service.error("Failed to disable SSO: " + e);
      this.setState({ isDisabling: false });
    }
  };

  render() {
    const { loading, loadError, currentUrl, isEditing, inputUrl, isSaving, isDisableModalOpen, isDisabling } =
      this.state;
    const enabled = currentUrl.trim() !== "";
    const saveDisabled = isSaving || inputUrl.trim() === "" || inputUrl.trim() === currentUrl.trim();

    return (
      <>
        <div className="settings-option-title">Single sign-on</div>
        <div className="settings-option-description">
          Connect a SAML identity provider (IdP) to enable single sign-on (SSO) for your organization. See the{" "}
          <TextLink href="https://www.buildbuddy.io/docs/config-auth/#saml-20">SAML setup guide</TextLink> for
          instructions on configuring your IdP.
        </div>
        {loading ? (
          <Spinner />
        ) : loadError ? (
          <div className="error-text">Failed to load SSO configuration: {loadError}</div>
        ) : enabled && !isEditing ? (
          <div className="sso-config">
            <p>Single sign-on is enabled.</p>
            <div className="sso-config-field">
              <label className="sso-config-label">IdP metadata URL</label>
              <TextInput className="sso-config-input" value={currentUrl} readOnly />
            </div>
            <div className="sso-config-actions">
              <FilledButton onClick={this.onClickChange}>Change</FilledButton>
              <OutlinedButton className="destructive" onClick={this.onClickDisable}>
                Disable
              </OutlinedButton>
            </div>
          </div>
        ) : (
          <form onSubmit={this.onSubmit} className="sso-config">
            {!enabled && <p>Single sign-on is not enabled.</p>}
            <div className="sso-config-field">
              <label className="sso-config-label">IdP metadata URL</label>
              <TextInput
                className="sso-config-input"
                placeholder="https://idp.example.com/metadata"
                value={inputUrl}
                onChange={this.handleInputChange}
              />
            </div>
            <div className="sso-config-actions">
              <FilledButton type="submit" disabled={saveDisabled}>
                {isSaving ? "Saving..." : enabled ? "Save" : "Enable"}
              </FilledButton>
              {isEditing && (
                <OutlinedButton type="button" onClick={this.onCancelEdit} disabled={isSaving}>
                  Cancel
                </OutlinedButton>
              )}
            </div>
          </form>
        )}
        <Modal isOpen={isDisableModalOpen} onRequestClose={this.onCloseDisableModal}>
          <Dialog className="sso-config-dialog">
            <DialogHeader>
              <DialogTitle>Confirm disabling SSO</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>Are you sure you want to disable single sign-on for this organization?</p>
              <p>Users will no longer be able to sign in through your identity provider.</p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {isDisabling && <Spinner />}
                <OutlinedButton onClick={this.onCloseDisableModal} disabled={isDisabling}>
                  Cancel
                </OutlinedButton>
                <FilledButton className="destructive" onClick={this.onConfirmDisable} disabled={isDisabling}>
                  Disable
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
