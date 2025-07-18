import { Share2 } from "lucide-react";
import React from "react";
import { acl } from "../../proto/acl_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import alert_service from "../alert/alert_service";
import { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import { FilledButton, OutlinedButton } from "../components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import Input from "../components/input/input";
import Modal from "../components/modal/modal";
import Select, { Option } from "../components/select/select";
import Spinner from "../components/spinner/spinner";
import rpcService from "../service/rpc_service";
import shortcuts, { KeyCombo } from "../shortcuts/shortcuts";
import InvocationModel from "./invocation_model";

export interface InvocationShareButtonComponentProps {
  user?: User;
  model: InvocationModel;
  invocationId: string;
}

interface State {
  isOpen: boolean;
  isLoading: boolean;
  acl?: acl.ACL;
  error?: string;
  keyboardShortcutHandle: string;
}

type VisibilitySelection = "group" | "public";

export default class InvocationShareButtonComponent extends React.Component<
  InvocationShareButtonComponentProps,
  State
> {
  state = this.getInitialState();

  private inputRef = React.createRef<HTMLInputElement>();

  componentDidMount() {
    let handle = shortcuts.register(KeyCombo.shift_c, () => {
      this.copyShareUrl();
    });
    this.setState({ keyboardShortcutHandle: handle });
  }

  componentDidUpdate(prevProps: InvocationShareButtonComponentProps) {
    if (prevProps.invocationId !== this.props.invocationId) {
      this.setState(this.getInitialState());
    }
  }

  componentWillUnmount() {
    shortcuts.deregister(this.state.keyboardShortcutHandle);
  }

  copyShareUrl() {
    navigator.clipboard.writeText(window.location.href);
    alert_service.success("Copied invocation link to clipboard");
  }

  private getInitialState(): State {
    return {
      isOpen: false,
      acl: this.props.model.invocation.acl ?? undefined,
      isLoading: false,
      keyboardShortcutHandle: "",
    };
  }

  private onShareButtonClick() {
    this.setState({ isOpen: true });
  }

  private onRequestClose() {
    this.setState({ isOpen: false });
  }

  private onLinkInputClick() {
    this.inputRef.current?.select();
  }

  private async onVisibilitySelectionChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const visibility = e.target.value as VisibilitySelection;
    const newAcl = new acl.ACL(this.props.model.invocation.acl ?? {});
    if (!newAcl.othersPermissions) {
      this.setState({ error: "Something went wrong. Refresh the page and try again." });
      return;
    }

    newAcl.othersPermissions.read = visibility === "public";

    this.setState({ acl: newAcl, isLoading: true });

    try {
      await rpcService.service.updateInvocation(
        new invocation.UpdateInvocationRequest({ invocationId: this.props.invocationId, acl: newAcl })
      );
    } catch (e) {
      console.error(e);
      this.setState({ error: "Something went wrong. Refresh the page and try again." });
    } finally {
      this.setState({ isLoading: false });
    }
  }

  private onCopyLinkButtonClick() {
    this.inputRef.current!.select();
    document.execCommand("copy");
  }

  render() {
    if (!capabilities.invocationSharing || !this.props.user) {
      return <></>;
    }
    const owningGroup = this.props.model.findOwnerGroup(this.props.user?.groups);
    const isEnabledByOrg = Boolean(owningGroup?.sharingEnabled);
    const isUnauthenticatedBuild = Boolean(
      !this.props.model.invocation.acl?.userId?.id && !this.props.model.invocation.acl?.groupId
    );
    const canChangePermissions = isEnabledByOrg && !isUnauthenticatedBuild;

    const visibility: VisibilitySelection = this.state.acl?.othersPermissions?.read ? "public" : "group";

    return (
      <>
        <FilledButton className="invocation-share-button" onClick={this.onShareButtonClick.bind(this)}>
          {/* TODO: Use an icon that signifies the current permissions */}
          <Share2 className="icon white" />
          Share
        </FilledButton>
        <Modal isOpen={this.state.isOpen} onRequestClose={this.onRequestClose.bind(this)}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Get link</DialogTitle>
            </DialogHeader>
            <DialogBody className="invocation-share-dialog">
              <div className="row">
                <Input
                  ref={this.inputRef}
                  readOnly={true}
                  value={window.location.href}
                  className="link-input"
                  onClick={this.onLinkInputClick.bind(this)}
                />
                <OutlinedButton onClick={this.onCopyLinkButtonClick.bind(this)}>Copy link</OutlinedButton>
              </div>
              <div>
                <div className="visibility-header">Visibility</div>
                <Select
                  onChange={this.onVisibilitySelectionChange.bind(this)}
                  value={visibility}
                  disabled={!canChangePermissions || this.state.isLoading || Boolean(this.state.error)}>
                  <Option value="group">{owningGroup?.name}</Option>
                  <Option value="public">Anyone with the link</Option>
                </Select>
                <div className="visibility-explanation">
                  {visibility === "group" && <>Anyone in this organization with the link can view</>}
                  {visibility === "public" && <>Anyone on the Internet with this link can view</>}
                </div>
              </div>
              {!canChangePermissions && (
                <div className="changing-permissions-disabled-explanation">
                  {isUnauthenticatedBuild ? (
                    <>Visibility cannot be changed since this build was performed by an unauthenticated user.</>
                  ) : (
                    <>Your organization does not allow editing build visibility.</>
                  )}
                </div>
              )}
              {this.state.error && <div className="error-message">{this.state.error}</div>}
            </DialogBody>
            <DialogFooter className="invocation-share-dialog-footer">
              <DialogFooterButtons>
                {this.state.isLoading && (
                  <>
                    <Spinner />
                    <span className="loading-message">Saving...</span>
                  </>
                )}
                <FilledButton onClick={this.onRequestClose.bind(this)}>Done</FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
