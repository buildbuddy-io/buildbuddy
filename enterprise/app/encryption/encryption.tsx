import React from "react";
import rpc_service from "../../../app/service/rpc_service";
import { encryption } from "../../../proto/encryption_ts_proto";
import Modal from "../../../app/components/modal/modal";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Spinner from "../../../app/components/spinner/spinner";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { BuildBuddyError } from "../../../app/util/errors";
import error_service from "../../../app/errors/error_service";
import TextInput from "../../../app/components/input/input";
import Link from "../../../app/components/link/link";

interface State {
  encryptionEnabled: boolean;
  supportedKMS: encryption.KMS[];

  // Disable encryption dialog.
  isDisableModalOpen: boolean;
  isDisablingInProgress: boolean;
  disablingError: BuildBuddyError | null;

  // Enable encryption form.
  selectedKMS: encryption.KMS;
  isEnablingInProgress: boolean;
  enablingError: BuildBuddyError | null;

  // Local KMS form.
  localKeyID: string;

  // GCP KMS form.
  gcpProject: string;
  gcpLocation: string;
  gcpKeyRing: string;
  gcpKey: string;

  // AWS KMS form.
  awsKeyARN: string;
}

export default class EncryptionComponent extends React.Component<{}, State> {
  state: State = {
    encryptionEnabled: false,
    supportedKMS: [],
    isDisableModalOpen: false,
    isDisablingInProgress: false,
    disablingError: null,
    selectedKMS: encryption.KMS.UNKNOWN_KMS,
    isEnablingInProgress: false,
    enablingError: null,
    localKeyID: "",
    gcpProject: "",
    gcpLocation: "",
    gcpKeyRing: "",
    gcpKey: "",
    awsKeyARN: "",
  };

  componentDidMount() {
    this.fetchConfig();
  }

  private async fetchConfig() {
    try {
      const response = await rpc_service.service.getEncryptionConfig(encryption.GetEncryptionConfigRequest.create());
      this.setState({
        encryptionEnabled: response.enabled,
        supportedKMS: response.supportedKms,
        selectedKMS: response.supportedKms[0],
      });
    } catch (e) {
      error_service.handleError(BuildBuddyError.parse(e));
    }
  }

  private onCloseDisableModal() {
    this.setState({ isDisableModalOpen: false });
  }

  private onClickDisable() {
    this.setState({ isDisableModalOpen: true });
  }

  private async onClickConfirmedDisable() {
    this.setState({ isDisablingInProgress: true, disablingError: null });
    try {
      await rpc_service.service.setEncryptionConfig(
        encryption.SetEncryptionConfigRequest.create({
          enabled: false,
        })
      );
      this.setState({ isDisableModalOpen: false });
      this.fetchConfig();
    } catch (e) {
      this.setState({ disablingError: BuildBuddyError.parse(e) });
    } finally {
      this.setState({ isDisablingInProgress: false });
    }
  }

  private async onClickEnable() {
    this.setState({ isEnablingInProgress: true, enablingError: null });
    let req = encryption.SetEncryptionConfigRequest.create({
      enabled: true,
    });
    switch (this.state.selectedKMS) {
      case encryption.KMS.LOCAL_INSECURE:
        req.kmsConfig = encryption.KMSConfig.create({
          localInsecureKmsConfig: encryption.LocalInsecureKMSConfig.create({
            keyId: this.state.localKeyID,
          }),
        });
        break;
      case encryption.KMS.GCP:
        req.kmsConfig = encryption.KMSConfig.create({
          gcpKmsConfig: encryption.GCPKMSConfig.create({
            project: this.state.gcpProject,
            location: this.state.gcpLocation,
            keyRing: this.state.gcpKeyRing,
            key: this.state.gcpKey,
          }),
        });
        break;
      case encryption.KMS.AWS:
        req.kmsConfig = encryption.KMSConfig.create({
          awsKmsConfig: encryption.AWSKMSConfig.create({
            keyArn: this.state.awsKeyARN,
          }),
        });
        break;
    }
    try {
      await rpc_service.service.setEncryptionConfig(req);
      this.setState({ isEnablingInProgress: false });
      this.fetchConfig();
    } catch (e) {
      this.setState({ enablingError: BuildBuddyError.parse(e) });
    } finally {
      this.setState({ isEnablingInProgress: false });
    }
  }

  private onSelectKMS(kms: encryption.KMS) {
    this.setState({ selectedKMS: kms });
  }

  private kmsName(kms: encryption.KMS) {
    switch (kms) {
      case encryption.KMS.LOCAL_INSECURE:
        return "Local KMS";
      case encryption.KMS.GCP:
        return "Google Cloud Platform KMS";
      case encryption.KMS.AWS:
        return "Amazon Web Services KMS";
      default:
        return "Unknown KMS";
    }
  }

  private onLocalIDChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ localKeyID: event.target.value });
  }

  private onGCPProjectChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ gcpProject: event.target.value });
  }

  private onGCPKeyRingChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ gcpKeyRing: event.target.value });
  }

  private onGCPKeyChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ gcpKey: event.target.value });
  }

  private onGCPLocationChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ gcpLocation: event.target.value });
  }

  private onAWSARNChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ awsKeyARN: event.target.value });
  }

  private renderKMSFields(kms: encryption.KMS) {
    switch (kms) {
      case encryption.KMS.LOCAL_INSECURE:
        return (
          <>
            <div className="field-row">
              <label htmlFor="localID" className="field-label">
                Key ID
              </label>
              <input
                autoComplete="off"
                type="text"
                name="localID"
                onChange={this.onLocalIDChange.bind(this)}
                value={this.state.localKeyID}
              />
            </div>
          </>
        );
      case encryption.KMS.GCP:
        return (
          <>
            <div className="kms-instructions">
              You must grant BuildBuddy permission to use the specified key for encryption and decryption via the
              following service account:
              <code>kms-prod@flame-build.iam.gserviceaccount.com</code>
              <div>
                The service account must be granted the following two permissions:
                <code>
                  cloudkms.cryptoKeyVersions.useToEncrypt
                  <br />
                  cloudkms.cryptoKeyVersions.useToDecrypt
                </code>
                These permissions are included in the following predefined role:
                <code>Cloud KMS CryptoKey Encrypter/Decrypter</code>
              </div>
            </div>
            <div className="field-row">
              <label htmlFor="gcpProject" className="field-label">
                Project ID
              </label>
              <input
                autoComplete="off"
                type="text"
                name="gcpProject"
                onChange={this.onGCPProjectChange.bind(this)}
                value={this.state.gcpProject}
              />
            </div>
            <div className="field-row">
              <label htmlFor="gcpKeyRing" className="field-label">
                Key Ring
              </label>
              <input
                autoComplete="off"
                type="text"
                name="gcpKeyRing"
                onChange={this.onGCPKeyRingChange.bind(this)}
                value={this.state.gcpKeyRing}
              />
            </div>
            <div className="field-row">
              <label htmlFor="gcpLocation" className="field-label">
                Key Ring Location
              </label>
              <input
                autoComplete="off"
                type="text"
                name="gcpLocation"
                onChange={this.onGCPLocationChange.bind(this)}
                value={this.state.gcpLocation}
              />
            </div>
            <div className="field-row">
              <label htmlFor="gcpKey" className="field-label">
                Key
              </label>
              <input
                autoComplete="off"
                type="text"
                name="gcpKey"
                onChange={this.onGCPKeyChange.bind(this)}
                value={this.state.gcpKey}
              />
            </div>
          </>
        );
      case encryption.KMS.AWS:
        return (
          <>
            <div className="kms-instructions">
              You must grant BuildBuddy permission to use the specified key for encryption and decryption by adding the
              following AWS account to the KMS key:
              <code>561871016185</code>
            </div>
            <div className="field-row">
              <label htmlFor="awsKeyARN" className="field-label">
                Key Resource Name (ARN)
              </label>
              <TextInput
                autoComplete="off"
                type="text"
                name="awsKeyARN"
                onChange={this.onAWSARNChange.bind(this)}
                value={this.state.awsKeyARN}
                placeholder="e.g. arn:aws:kms:us-east-1:123456789:key/123456"
                className="aws-key-arn"
              />
            </div>
          </>
        );
    }
  }

  private renderKMSTabs(options: encryption.KMS[], selected: encryption.KMS) {
    return (
      <div className="kms-tabs">
        {options.map((kms, idx) => (
          <Link
            className={`kms-tab ${selected === kms ? "active-tab" : ""}`}
            onClick={this.onSelectKMS.bind(this, kms)}>
            {this.kmsName(kms)}
          </Link>
        ))}
      </div>
    );
  }

  render() {
    return (
      <>
        {this.state.encryptionEnabled && (
          <div>
            <p>Customer-managed encryption keys are enabled.</p>

            <FilledButton className="destructive" onClick={this.onClickDisable.bind(this)}>
              Disable
            </FilledButton>
            <Modal isOpen={this.state.isDisableModalOpen} onRequestClose={this.onCloseDisableModal.bind(this)}>
              <Dialog className="invocation-delete-dialog">
                <DialogHeader>
                  <DialogTitle>Confirm disabling encryption</DialogTitle>
                </DialogHeader>
                <DialogBody>
                  <p>Are you sure you want to disable encryption using the customer-managed key?</p>
                  <p>Previously encrypted artifacts stored in the cache will no longer be accessible.</p>
                  <p>This action is irreversible. </p>
                  {this.state.disablingError && (
                    <div className="error-description">{this.state.disablingError.description}</div>
                  )}
                </DialogBody>
                <DialogFooter>
                  <DialogFooterButtons>
                    {this.state.isDisablingInProgress && <Spinner />}
                    <OutlinedButton
                      disabled={this.state.isDisablingInProgress}
                      onClick={this.onCloseDisableModal.bind(this)}>
                      Cancel
                    </OutlinedButton>
                    <FilledButton
                      className="destructive"
                      disabled={this.state.isDisablingInProgress}
                      onClick={this.onClickConfirmedDisable.bind(this)}>
                      Disable
                    </FilledButton>
                  </DialogFooterButtons>
                </DialogFooter>
              </Dialog>
            </Modal>
          </div>
        )}
        {!this.state.encryptionEnabled && (
          <div>
            <p>Customer-managed encryption keys are not currently enabled for this organization.</p>
            <p>
              To enable the use of customer-managed encryption keys, you will need to provide a reference to a key
              managed by a supported Key Management System.
            </p>
            <form className="kms-form">
              {this.renderKMSTabs(this.state.supportedKMS, this.state.selectedKMS)}
              {this.state.selectedKMS != encryption.KMS.UNKNOWN_KMS && (
                <div className="kms-fields">{this.renderKMSFields(this.state.selectedKMS)}</div>
              )}
              <FilledButton
                disabled={this.state.selectedKMS == encryption.KMS.UNKNOWN_KMS || this.state.isEnablingInProgress}
                onClick={this.onClickEnable.bind(this)}>
                Enable
              </FilledButton>
              {this.state.enablingError && <div className="form-error">{this.state.enablingError.description}</div>}
            </form>
          </div>
        )}
      </>
    );
  }
}
