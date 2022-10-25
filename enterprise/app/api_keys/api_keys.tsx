import { Key } from "lucide-react";
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
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { api_key } from "../../../proto/api_key_ts_proto";

export interface ApiKeysComponentProps {
  user?: User;
}

interface State {
  initialLoadError: string | null;
  getApiKeysResponse: api_key.GetApiKeysResponse | null;

  createForm: FormState<api_key.CreateApiKeyRequest> | null;

  updateForm: FormState<api_key.UpdateApiKeyRequest> | null;

  keyToDelete: api_key.ApiKey | null;
  isDeleteModalOpen: boolean;
  isDeleteModalSubmitting: boolean;
}

const INITIAL_STATE: State = {
  initialLoadError: null,
  getApiKeysResponse: null,

  createForm: {},

  updateForm: {},

  keyToDelete: null,
  isDeleteModalOpen: false,
  isDeleteModalSubmitting: false,
};

type ApiKeyFields = api_key.ICreateApiKeyRequest | api_key.IUpdateApiKeyRequest;

type FormState<T extends ApiKeyFields> = {
  isOpen?: boolean;
  isSubmitting?: boolean;
  request?: T;
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
      const response = await rpcService.service.getApiKeys({ groupId: this.props.user.selectedGroup.id });
      this.setState({ getApiKeysResponse: response });
    } catch (e) {
      this.setState({ initialLoadError: BuildBuddyError.parse(e).description });
    } finally {
      this.setState({ createForm: {}, updateForm: {} });
    }
  }

  // Creation modal

  private async onClickCreateNew() {
    this.setState({
      createForm: {
        isOpen: true,
        request: new api_key.CreateApiKeyRequest({
          capability: [api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY],
        }),
      },
    });
    setTimeout(() => {
      this.createFormRef.current?.querySelector("input")?.focus();
    });
  }
  private async onCloseCreateForm() {
    this.setState({ createForm: {} });
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
      await rpcService.service.createApiKey(
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
    this.setState({ updateForm: {} });
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
      await rpcService.service.updateApiKey(
        new api_key.CreateApiKeyRequest({
          ...this.state.updateForm.request,
          groupId: this.props.user.selectedGroup.id,
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
      await rpcService.service.deleteApiKey(new api_key.DeleteApiKeyRequest({ id: this.state.keyToDelete.id }));
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

  private onChangeCapability<T extends ApiKeyFields>(
    request: T,
    capability: api_key.ApiKey.Capability,
    enabled: boolean,
    onChange: (name: string, value: any) => any
  ) {
    if (enabled) {
      request.capability.push(capability);
    } else {
      request.capability = request.capability.filter((cap) => cap !== capability);
    }
    onChange("capability", request.capability);
  }

  private onChangeReadOnly<T extends ApiKeyFields>(
    request: T,
    onChange: (name: string, value: any) => any,
    e: React.ChangeEvent<HTMLInputElement>
  ) {
    this.onChangeCapability(request, api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY, false, onChange);
    this.onChangeCapability(request, api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY, false, onChange);
  }

  private onChangeCASOnly<T extends ApiKeyFields>(
    request: T,
    onChange: (name: string, value: any) => any,
    e: React.ChangeEvent<HTMLInputElement>
  ) {
    this.onChangeCapability(request, api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY, false, onChange);
    this.onChangeCapability(request, api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY, true, onChange);
  }

  private onChangeReadWrite<T extends ApiKeyFields>(
    request: T,
    onChange: (name: string, value: any) => any,
    e: React.ChangeEvent<HTMLInputElement>
  ) {
    this.onChangeCapability(request, api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY, true, onChange);
    this.onChangeCapability(request, api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY, false, onChange);
  }

  private onChangeVisibility<T extends ApiKeyFields>(
    request: T,
    onChange: (name: string, value: any) => any,
    e: React.ChangeEvent<HTMLInputElement>
  ) {
    onChange("visibleToDevelopers", e.target.checked);
  }

  private onChangeRegisterExecutor<T extends ApiKeyFields>(
    request: T,
    onChange: (name: string, value: any) => any,
    e: React.ChangeEvent<HTMLInputElement>
  ) {
    this.onChangeCapability(
      request,
      api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY,
      e.target.checked,
      onChange
    );
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
    onSubmit: () => any;
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
                <TextInput name="label" onChange={this.onChangeLabel.bind(this, onChange)} value={request?.label} />
              </div>
              <div className="field-container">
                <label className="checkbox-row">
                  <input
                    type="radio"
                    onChange={this.onChangeReadWrite.bind(this, request, onChange)}
                    checked={isReadWrite(request)}
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
                    onChange={this.onChangeReadOnly.bind(this, request, onChange)}
                    checked={isReadOnly(request)}
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
                    onChange={this.onChangeCASOnly.bind(this, request, onChange)}
                    checked={isCASOnly(request)}
                  />
                  <span>
                    CAS-only key <span className="field-description">(disable action cache uploads)</span>
                  </span>
                </label>
              </div>

              {capabilities.executorKeyCreation && (
                <div className="field-container">
                  <label className="checkbox-row">
                    <input
                      type="checkbox"
                      onChange={this.onChangeRegisterExecutor.bind(this, request, onChange)}
                      checked={hasCapability(request, api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY)}
                    />
                    <span>
                      Executor key <span className="field-description">(for self-hosted executors)</span>
                    </span>
                  </label>
                </div>
              )}
              <div className="field-container">
                <label className="checkbox-row">
                  <input
                    type="checkbox"
                    onChange={this.onChangeVisibility.bind(this, request, onChange)}
                    checked={isVisibleToDevelopers(request)}
                  />
                  <span>
                    Visible to developers <span className="field-description">(users with the role Developer)</span>
                  </span>
                </label>
              </div>
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
        {this.props.user.canCall("createApiKey") && (
          <div>
            <FilledButton className="big-button" onClick={this.onClickCreateNew.bind(this)}>
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
          {getApiKeysResponse.apiKey.length == 0 && !this.props.user.canCall("createApiKey") && (
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
              <div className="api-key-value">
                <Key className="icon" />
                <span>{key.value}</span>
              </div>
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

function hasCapability<T extends ApiKeyFields>(apiKey: T | null, capability: api_key.ApiKey.Capability) {
  return Boolean(apiKey?.capability?.some((existingCapability) => existingCapability === capability));
}

function isReadWrite<T extends ApiKeyFields>(apiKey: T | null) {
  return hasCapability(apiKey, api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY);
}

function isCASOnly<T extends ApiKeyFields>(apiKey: T | null) {
  return hasCapability(apiKey, api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY);
}

function isReadOnly<T extends ApiKeyFields>(apiKey: T | null) {
  return (
    !hasCapability(apiKey, api_key.ApiKey.Capability.CACHE_WRITE_CAPABILITY) &&
    !hasCapability(apiKey, api_key.ApiKey.Capability.CAS_WRITE_CAPABILITY)
  );
}

function isVisibleToDevelopers<T extends ApiKeyFields>(apiKey: T | null) {
  return apiKey?.visibleToDevelopers;
}

function describeCapabilities<T extends ApiKeyFields>(apiKey: T | null) {
  let capabilities = "Read+Write";
  if (isReadOnly(apiKey)) {
    capabilities = "Read-only";
  }
  if (isCASOnly(apiKey)) {
    capabilities = "CAS-only";
  }
  if (hasCapability(apiKey, api_key.ApiKey.Capability.REGISTER_EXECUTOR_CAPABILITY)) {
    capabilities += "+Executor";
  }
  if (isVisibleToDevelopers(apiKey)) {
    capabilities += " [D]";
  }
  return capabilities;
}
