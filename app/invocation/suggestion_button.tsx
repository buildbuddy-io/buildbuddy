import React from "react";
import Button, { OutlinedButton } from "../components/button/button";
import { CancelablePromise } from "../../app/service/rpc_service";
import { OutlinedButtonGroup } from "../components/button/button_group";
import Modal from "../components/modal/modal";
import Dialog, {
  DialogHeader,
  DialogTitle,
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
} from "../components/dialog/dialog";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup, { PopupContainer } from "../components/popup/popup";
import InvocationModel from "./invocation_model";
import Spinner from "../components/spinner/spinner";
import { Bot, ChevronDown } from "lucide-react";
import capabilities from "../capabilities/capabilities";
import { User } from "../auth/user";

export interface SuggestionButtonProps {
  model: InvocationModel;
  user: User | undefined;
}

type State = {
  isMenuOpen: boolean;
  isDialogOpen: boolean;
  isLoading: boolean;
};

const openAIAgreedLocalStorageKey = "agreedToUseOpenAI";
export default class SuggestionButton extends React.Component<SuggestionButtonProps, State> {
  state: State = {
    isMenuOpen: false,
    isDialogOpen: false,
    isLoading: false,
  };

  private inFlightRpc: CancelablePromise | undefined;

  private onOpenMenu() {
    this.setState({ isMenuOpen: true });
  }
  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private onOpenDialog() {
    this.setState({ isMenuOpen: false, isDialogOpen: true });
  }
  private onCloseDialog() {
    this.setState({ isDialogOpen: false });
  }

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  onClickAsk(service: string) {
    if (localStorage.getItem(openAIAgreedLocalStorageKey)) {
      this.makeRequest("");
      return;
    }

    this.onOpenDialog();
  }

  makeRequest(service: string) {
    localStorage.setItem(openAIAgreedLocalStorageKey, "true");
    this.setState({ isLoading: true, isMenuOpen: false, isDialogOpen: false });
    this.inFlightRpc = this.props.model.fetchSuggestions(service).finally(() => this.setState({ isLoading: false }));
  }

  render() {
    if (
      !capabilities.config.botSuggestionsEnabled ||
      !this.props.user ||
      !this.props.user?.selectedGroup?.botSuggestionsEnabled ||
      this.props.model.getStatus() != "Failed"
    ) {
      return <></>;
    }

    return (
      <>
        <PopupContainer>
          <OutlinedButtonGroup>
            <OutlinedButton
              disabled={this.state.isLoading}
              className="workflow-rerun-button"
              onClick={this.onClickAsk.bind(this, "")}>
              {this.state.isLoading ? <Spinner /> : <Bot />}
              <span>{this.state.isLoading ? "Thinking..." : "Ask Buddy"}</span>
            </OutlinedButton>
            {capabilities.config.multipleSuggestionProviders && (
              <OutlinedButton
                disabled={this.state.isLoading}
                className="icon-button"
                onClick={this.onOpenMenu.bind(this)}>
                <ChevronDown />
              </OutlinedButton>
            )}
          </OutlinedButtonGroup>
          <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="right">
            <Menu>
              <MenuItem onClick={this.makeRequest.bind(this, "google")}>Ask Buddy (Powered by Google)</MenuItem>
              <MenuItem onClick={this.onOpenDialog.bind(this)}>Ask Buddy (Powered by OpenAI)</MenuItem>
            </Menu>
          </Popup>
        </PopupContainer>
        <Modal isOpen={this.state.isDialogOpen} onRequestClose={this.onCloseDialog.bind(this)}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Confirm using OpenAI</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>Asking Buddy will send the redacted build logs of the current invocation to OpenAI.</p>
              <p>
                Please make sure there is no sensitive data in your build logs, and that you are okay with sending this
                data to OpenAI before pressing OK.
              </p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <OutlinedButton onClick={this.onCloseDialog.bind(this)}>Cancel</OutlinedButton>
                <Button onClick={this.makeRequest.bind(this, "openai")}>OK</Button>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
