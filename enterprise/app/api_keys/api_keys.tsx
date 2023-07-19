import { Check, Copy, Eye, EyeOff, Key } from "lucide-react";
import React from "react";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import TextInput from "../../../app/components/input/input";
import Spinner from "../../../app/components/spinner/spinner";
import Modal from "../../../app/components/modal/modal";
import alert_service from "../../../app/alert/alert_service";
import { copyToClipboard } from "../../../app/util/clipboard";
import errorService from "../../../app/errors/error_service";
import { CancelableRpc } from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { api_key } from "../../../proto/api_key_ts_proto";

export interface ApiKeysComponentProps {
  /** The authenticated user. */
  user: User;

  /** Whether to show only user-owned keys. */
  userOwnedOnly?: boolean;

  get: CancelableRpc<api_key.GetApiKeysRequest, api_key.GetApiKeysResponse>;
  create: CancelableRpc<api_key.CreateApiKeyRequest, api_key.CreateApiKeyResponse>;
  update: CancelableRpc<api_key.UpdateApiKeyRequest, api_key.UpdateApiKeyResponse>;
  delete: CancelableRpc<api_key.DeleteApiKeyRequest, api_key.DeleteApiKeyResponse>;
}

interface State {
  initialLoadError: string | null;
  getApiKeysResponse: api_key.GetApiKeysResponse | null;

  createForm: FormState<api_key.CreateApiKeyRequest>;

  updateForm: FormState<api_key.UpdateApiKeyRequest>;

  keyToDelete: api_key.ApiKey | null;
  isDeleteModalOpen: boolean;
  isDeleteModalSubmitting: boolean;
}

const INITIAL_STATE: State = {
  initialLoadError: null,
  getApiKeysResponse: null,

  createForm: newFormState(api_key.CreateApiKeyRequest.create()),

  updateForm: newFormState(api_key.UpdateApiKeyRequest.create()),

  keyToDelete: null,
  isDeleteModalOpen: false,
  isDeleteModalSubmitting: false,
};

type ApiKeyFields = api_key.ICreateApiKeyRequest | api_key.IUpdateApiKeyRequest;

type FormState<T extends ApiKeyFields> = {
  isOpen: boolean;
  isSubmitting: boolean;
  request: T;
};

export default class ApiKeysComponent extends React.Component<ApiKeysComponentProps, State> {
  state: State = INITIAL_STATE;

  private createFormRef = React.createRef<HTMLFormElement>();
  private updateFormRef = React.createRef<HTMLFormElement>();
  private deleteButtonRef = React.createRef<HTMLButtonElement>();

  componentDidMount() {
    this.fetchApiKeys();
  }

  componentDidUpdate(prevProps: ApiKeysComponentProps) {
    if (prevProps.user !== this.props.user) {
      this.setState(INITIAL_STATE);
      const _ = this.fetchApiKeys();
    }
  }

  private async fetchApiKeys() {
    if (!this.props.user) return;

    try {
      const response = await this.props.get(
        api_key.GetApiKeysRequest.create({
          groupId: this.props.user.selectedGroup.id,
        })
      );
      this.setState({ getApiKeysResponse: response });
    } catch (e) {
      this.setState({ initialLoadError: BuildBuddyError.parse(e).description });
    } finally {
      this.setState({
        createForm: newFormState(api_key.CreateApiKeyRequest.create()),
        updateForm: newFormState(api_key.UpdateApiKeyRequest.create()),
      });
    }
  }

  private defaultCapabilities(): api_key.ApiKey.Capability[] {
    if (this.props.user.isGroupAdmin()) {
      return [api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY];
    }
    return [api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY];
  }

  // Creation modal

  private async onClickCreateNew() {
    this.setState({
      createForm: {
        isOpen: true,
        isSubmitting: false,
        request: new api_key.CreateApiKeyRequest({
          capability: this.defaultCapabilities(),
        }),
      },
    });
    setTimeout(() => {
      this.createFormRef.current?.querySelector("input")?.focus();
    });
  }
  private async onCloseCreateForm() {
    this.setState({ createForm: newFormState(api_key.CreateApiKeyRequest.create()) });
  }
  private onChangeCreateForm(name: string, value: any) {
    this.setState({
      createForm: {
        ...this.state.createForm,
        request: new api_key.CreateApiKeyRequest({ ...this.state.createForm.request, [name]: value }),
      },
    });
  }
  private async onSubmitCreateNewForm(e: React.FormEvent) {
    e.preventDefault();
    if (!this.props.user) return;

    try {
      this.setState({ createForm: { ...this.state.createForm, isSubmitting: true } });
      await this.props.create(
        new api_key.CreateApiKeyRequest({
          ...this.state.createForm.request,
          groupId: this.props.user.selectedGroup.id,
        })
      );
    } catch (e) {
      this.setState({ createForm: { ...this.state.createForm, isSubmitting: false } });
      errorService.handleError(e);
      return;
    }

    await this.fetchApiKeys();
  }

  // Update modal

  private async onClickUpdate(apiKey: api_key.ApiKey) {
    this.setState({
      updateForm: {
        isOpen: true,
        isSubmitting: false,
        request: new api_key.UpdateApiKeyRequest({
          id: apiKey.id,
          label: apiKey.label,
          capability: [...apiKey.capability],
          visibleToDevelopers: apiKey.visibleToDevelopers,
        }),
      },
    });
    setTimeout(() => {
      this.updateFormRef.current?.querySelector("input")?.focus();
    });
  }
  private async onCloseUpdateForm() {
    this.setState({ updateForm: newFormState(api_key.UpdateApiKeyRequest.create()) });
  }
  private onChangeUpdateForm(name: string, value: any) {
    this.setState({
      updateForm: {
        ...this.state.updateForm,
        request: new api_key.UpdateApiKeyRequest({ ...this.state.updateForm.request, [name]: value }),
      },
    });
  }
  private async onSubmitUpdateForm(e: React.FormEvent) {
    e.preventDefault();
    if (!this.props.user) return;

    try {
      this.setState({ updateForm: { ...this.state.updateForm, isSubmitting: true } });
      await this.props.update(
        api_key.UpdateApiKeyRequest.create({
          ...this.state.updateForm.request,
        })
      );
    } catch (e) {
      this.setState({ updateForm: { ...this.state.updateForm, isSubmitting: false } });
      errorService.handleError(e);
      return;
    }

    await this.fetchApiKeys();
  }

  // Delete modal

  private onClickDelete(keyToDelete: api_key.ApiKey) {
    this.setState({ keyToDelete, isDeleteModalOpen: true });
    setTimeout(() => {
      this.deleteButtonRef.current?.focus();
    });
  }
  private onCloseDeleteModal() {
    if (!this.state.isDeleteModalSubmitting) {
      this.setState({ isDeleteModalOpen: false });
    }
  }
  private async onConfirmDelete() {
    try {
      this.setState({ isDeleteModalSubmitting: true });
      await this.props.delete(new api_key.DeleteApiKeyRequest({ id: this.state.keyToDelete!.id }));
      await this.fetchApiKeys();
      this.setState({ isDeleteModalOpen: false });
    } catch (e) {
      errorService.handleError(e);
    } finally {
      this.setState({ isDeleteModalSubmitting: false });
    }
  }

  private onChangeLabel(onChange: (name: string, value: any) => any, e: React.ChangeEvent<HTMLInputElement>) {
    onChange(e.target.name, e.target.value);
  }

  private onSelectReadOnly(onChange: (name: string, value: any) => any) {
    onChange("capability", []);
  }

  private onSelectCASOnly(onChange: (name: string, value: any) => any) {
    onChange("capability", [api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY]);
  }

  private onSelectReadWrite(onChange: (name: string, value: any) => any) {
    onChange("capability", [api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY]);
  }

  private onSelectExecutor(onChange: (name: string, value: any) => any) {
    onChange("capability", [
      api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY,
      api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY,
    ]);
  }

  private onChangeVisibility(onChange: (name: string, value: any) => any, e: React.ChangeEvent<HTMLInputElement>) {
    onChange("visibleToDevelopers", e.target.checked);
  }

  private canChangeCapabilities(): boolean {
    return this.props.user.isGroupAdmin();
  }

  private canEdit(): boolean {
    return this.props.userOwnedOnly || this.props.user.canCall("updateApiKey");
  }

  private renderModal<T extends ApiKeyFields>({
    title,
    submitLabel,
    onRequestClose,
    onSubmit,
    onChange,
    ref,
    formState: { request, isOpen, isSubmitting },
  }: {
    title: string;
    submitLabel: string;
    onRequestClose: () => any;
    onSubmit: (e: React.FormEvent) => any;
    onChange: (name: string, value: any) => any;
    ref: React.RefObject<HTMLFormElement>;
    formState: FormState<T>;
  }) {
    return (
      <Modal isOpen={isOpen} onRequestClose={onRequestClose} shouldFocusAfterRender={false}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
          </DialogHeader>
          <form ref={ref} className="api-keys-form" onSubmit={onSubmit}>
            <DialogBody>
              <div className="field-container">
                <label className="note-input-label" htmlFor="label">
                  Label <span className="field-description">(what's this key for?)</span>
                </label>
                <TextInput
                  name="label"
                  onChange={this.onChangeLabel.bind(this, onChange)}
                  value={request?.label || ""}
                />
              </div>
              <div className="field-container">
                <label className="checkbox-row">
                  <input
                    type="radio"
                    onChange={this.onSelectReadWrite.bind(this, onChange)}
                    checked={isReadWrite(request)}
                    disabled={!this.canChangeCapabilities()}
                  />
                  <span>
                    Read+Write key <span className="field-description">(allow all remote cache uploads)</span>
                  </span>
                </label>
              </div>
              <div className="field-container">
                <label className="checkbox-row">
                  <input
                    type="radio"
                    onChange={this.onSelectReadOnly.bind(this, onChange)}
                    checked={isReadOnly(request)}
                    disabled={!this.canChangeCapabilities()}
                  />
                  <span>
                    Read-only key <span className="field-description">(disable all remote cache uploads)</span>
                  </span>
                </label>
              </div>
              <div className="field-container">
                <label className="checkbox-row">
                  <input
                    type="radio"
                    onChange={this.onSelectCASOnly.bind(this, onChange)}
                    checked={isCASOnly(request)}
                    disabled={!this.canChangeCapabilities()}
                    debug-id="cas-only-radio-button"
                  />
                  <span>
                    CAS-only key <span className="field-description">(disable action cache uploads)</span>
                  </span>
                </label>
              </div>

              {/* User-owned keys cannot be used to register executors. */}
              {capabilities.executorKeyCreation && !this.props.userOwnedOnly && (
                <div className="field-container">
                  <label className="checkbox-row">
                    <input
                      type="radio"
                      onChange={this.onSelectExecutor.bind(this, onChange)}
                      checked={isExecutorKey(request)}
                      disabled={!this.canChangeCapabilities()}
                    />
                    <span>
                      Executor key <span className="field-description">(for self-hosted executors)</span>
                    </span>
                  </label>
                </div>
              )}
              {/* "Visible to developers" bit does not apply for user-level keys. */}
              {!this.props.userOwnedOnly && (
                <div className="field-container">
                  <label className="checkbox-row">
                    <input
                      type="checkbox"
                      onChange={this.onChangeVisibility.bind(this, onChange)}
                      checked={request.visibleToDevelopers}
                    />
                    <span>
                      Visible to developers <span className="field-description">(users with the role Developer)</span>
                    </span>
                  </label>
                </div>
              )}
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {isSubmitting && <Spinner />}
                <OutlinedButton type="button" onClick={onRequestClose}>
                  Cancel
                </OutlinedButton>
                <FilledButton type="submit" disabled={isSubmitting}>
                  {submitLabel}
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </form>
        </Dialog>
      </Modal>
    );
  }

  render() {
    if (!this.props.user) return <></>;

    const { keyToDelete, createForm, updateForm, getApiKeysResponse, isDeleteModalOpen, initialLoadError } = this.state;

    if (!getApiKeysResponse) {
      return (
        <div className="api-keys">
          {this.state.initialLoadError ? (
            <div className="error-container">{initialLoadError}</div>
          ) : (
            <div className="loading" />
          )}
        </div>
      );
    }

    return (
      <div className="api-keys">
        {this.canEdit() && (
          <div>
            <FilledButton
              className="big-button"
              onClick={this.onClickCreateNew.bind(this)}
              debug-id="create-new-api-key">
              Create new API key
            </FilledButton>
          </div>
        )}

        {this.renderModal({
          title: "New API key",
          submitLabel: "Create",
          formState: createForm,
          ref: this.createFormRef,
          onChange: this.onChangeCreateForm.bind(this),
          onSubmit: this.onSubmitCreateNewForm.bind(this),
          onRequestClose: this.onCloseCreateForm.bind(this),
        })}
        {this.renderModal({
          title: "Edit API key",
          submitLabel: "Save",
          formState: updateForm,
          ref: this.updateFormRef,
          onChange: this.onChangeUpdateForm.bind(this),
          onSubmit: this.onSubmitUpdateForm.bind(this),
          onRequestClose: this.onCloseUpdateForm.bind(this),
        })}

        <div className="api-keys-list">
          {!this.props.userOwnedOnly && getApiKeysResponse.apiKey.length == 0 && !this.canEdit() && (
            <div className="no-api-keys-message">
              No API keys have been made visible to developers. Only organization admins can create API keys.
            </div>
          )}
          {getApiKeysResponse.apiKey.map((key) => (
            <div key={key.id} className="api-key-list-item">
              <div className="api-key-label">
                {key.label ? (
                  <span title={key.label}>{key.label}</span>
                ) : (
                  <span className="untitled-key">Untitled key</span>
                )}
              </div>
              <div className="api-key-capabilities">
                <span>{describeCapabilities(key)}</span>
              </div>
              <ApiKeyField value={key.value} />
              {this.props.user.canCall("updateApiKey") && (
                <OutlinedButton className="api-key-edit-button" onClick={this.onClickUpdate.bind(this, key)}>
                  Edit
                </OutlinedButton>
              )}
              {this.props.user.canCall("deleteApiKey") && (
                <OutlinedButton onClick={this.onClickDelete.bind(this, key)} className="destructive">
                  Delete
                </OutlinedButton>
              )}
            </div>
          ))}
        </div>

        <Modal
          className="api-keys-delete-modal"
          isOpen={Boolean(isDeleteModalOpen)}
          onRequestClose={this.onCloseDeleteModal.bind(this)}
          shouldFocusAfterRender={false}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Confirm deletion</DialogTitle>
            </DialogHeader>
            <DialogBody>
              Are you sure you want to delete the API key{" "}
              <span className="delete-modal-key-value">{keyToDelete?.value}</span>
              {keyToDelete?.label && (
                <>
                  {" "}
                  (<span className="delete-modal-key-label">{keyToDelete.label}</span>)
                </>
              )}
              ? This action cannot be undone.
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {this.state.isDeleteModalSubmitting && <Spinner />}
                <OutlinedButton
                  disabled={this.state.isDeleteModalSubmitting}
                  onClick={this.onCloseDeleteModal.bind(this)}>
                  Cancel
                </OutlinedButton>
                <FilledButton
                  ref={this.deleteButtonRef}
                  className="destructive"
                  disabled={this.state.isDeleteModalSubmitting}
                  onClick={this.onConfirmDelete.bind(this)}>
                  Delete
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </div>
    );
  }
}

function capabilitiesToInt(capabilities: api_key.ApiKey.Capability[]): number {
  let out = 0;
  for (const capability of capabilities) {
    out |= capability;
  }
  return out;
}

function hasExactCapabilities<T extends ApiKeyFields>(apiKey: T | null, capabilities: api_key.ApiKey.Capability[]) {
  return capabilitiesToInt(apiKey?.capability || []) === capabilitiesToInt(capabilities);
}

function isReadWrite<T extends ApiKeyFields>(apiKey: T | null) {
  return hasExactCapabilities(apiKey, [api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY]);
}

function isCASOnly<T extends ApiKeyFields>(apiKey: T | null) {
  return hasExactCapabilities(apiKey, [api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY]);
}

function isExecutorKey<T extends ApiKeyFields>(apiKey: T | null) {
  return hasExactCapabilities(apiKey, [
    api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY,
    api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY,
  ]);
}

function isReadOnly<T extends ApiKeyFields>(apiKey: T | null) {
  return hasExactCapabilities(apiKey, []);
}

function describeCapabilities<T extends ApiKeyFields>(apiKey: T) {
  let capabilities = "Read+Write";
  if (isReadOnly(apiKey)) {
    capabilities = "Read-only";
  } else if (isCASOnly(apiKey)) {
    capabilities = "CAS-only";
  } else if (isExecutorKey(apiKey)) {
    capabilities = "Executor";
  }
  if (apiKey.visibleToDevelopers) {
    capabilities += " [D]";
  }
  return capabilities;
}

function newFormState<T extends ApiKeyFields>(request: T): FormState<T> {
  return {
    isOpen: false,
    isSubmitting: false,
    request,
  };
}

interface ApiKeyFieldProps {
  value: string;
}

interface ApiKeyFieldState {
  isCopied: boolean;
  hideValue: boolean;
  displayValue: string;
}

const ApiKeyFieldDefaultState: ApiKeyFieldState = {
  isCopied: false,
  hideValue: true,
  displayValue: "************",
};

class ApiKeyField extends React.Component<ApiKeyFieldProps, ApiKeyFieldState> {
  state = ApiKeyFieldDefaultState;

  // onClick handler function for the copy button
  private handleCopyClick() {
    copyToClipboard(this.props.value);
    this.setState({ isCopied: true }, () => {
      alert_service.success("Copied API key to clipboard");
    });
    setTimeout(() => {
      this.setState({ isCopied: false });
    }, 4000);
  }

  // onClick handler function for the hide/reveal button
  private toggleHideValue() {
    this.setState({
      hideValue: !this.state.hideValue,
      displayValue: this.state.hideValue ? this.props.value : ApiKeyFieldDefaultState.displayValue,
    });
  }

  render() {
    const { isCopied, hideValue, displayValue } = this.state;

    return (
      <div className="api-key-value">
        <span>{displayValue}</span>
        <button className="api-key-value-copy" onClick={this.handleCopyClick.bind(this)} disabled={isCopied}>
          {isCopied ? <Check style={{ stroke: "green" }} className="icon" /> : <Copy className="icon" />}
        </button>
        <button className="api-key-value-hide" onClick={this.toggleHideValue.bind(this)}>
          {hideValue ? <Eye className="icon" /> : <EyeOff className="icon" />}
        </button>
      </div>
    );
  }
}
