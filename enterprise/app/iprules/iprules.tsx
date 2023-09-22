import React from "react";
import { User } from "../../../app/auth/auth_service";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { iprules } from "../../../proto/iprules_ts_proto";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import TextInput from "../../../app/components/input/input";
import capabilities from "../../../app/capabilities/capabilities";
import Spinner from "../../../app/components/spinner/spinner";
import Modal from "../../../app/components/modal/modal";
import { ru } from "date-fns/locale";
import rpc_service from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { grp } from "../../../proto/group_ts_proto";

export interface Props {
  user: User;
}

interface State {
  enforcementEnabled: boolean;
  rules: Array<iprules.IPRule>;

  editModalOpen: boolean;
  editModalTitle: string;
  editModalRule: iprules.IPRule;
  editModalSubmitLabel: string;
  editModalSubmitting: boolean;
  editModalError: string;

  deleteModalOpen: boolean;
  deleteModalRule: iprules.IPRule;
  deleteModalSubmitting: boolean;

  bulkModalOpen: boolean;
  bulkModalSubmitting: boolean;
  bulkModalText: string;
  bulkModalError: string;
}

export default class IpRulesComponent extends React.Component<Props, State> {
  state: State = {
    enforcementEnabled: false,
    rules: [],

    editModalOpen: false,
    editModalTitle: "",
    editModalRule: iprules.IPRule.create(),
    editModalSubmitLabel: "",
    editModalSubmitting: false,
    editModalError: "",

    deleteModalOpen: false,
    deleteModalRule: iprules.IPRule.create(),
    deleteModalSubmitting: false,

    bulkModalOpen: false,
    bulkModalSubmitting: false,
    bulkModalText: "",
    bulkModalError: "",
  };

  componentDidMount() {
    this.fetchIPRules();
  }

  private async fetchIPRules() {
    if (!this.props.user) return;

    rpcService.service
      .getIPRulesConfig(iprules.GetRulesConfigRequest.create())
      .then((c) => this.setState({ enforcementEnabled: c.enforceIpRules }))
      .catch((e) => errorService.handleError(e));

    rpcService.service
      .getIPRules(iprules.GetRulesRequest.create())
      .then((r) => this.setState({ rules: r.ipRules }))
      .catch((e) => errorService.handleError(e));
  }

  private onCloseEditModal() {
    this.setState({
      editModalOpen: false,
      editModalSubmitting: false,
      editModalError: "",
    });
  }

  private async onSubmitEditModal(e: React.FormEvent) {
    e.preventDefault();
    this.setState({ editModalSubmitting: true });

    try {
      if (!this.state.editModalRule.ipRuleId) {
        await rpc_service.service.addIPRule(iprules.AddRuleRequest.create({ rule: this.state.editModalRule }));
      } else {
        await rpc_service.service.updateIPRule(iprules.UpdateRuleRequest.create({ rule: this.state.editModalRule }));
      }
      this.onCloseEditModal();
    } catch (e) {
      this.setState({ editModalError: BuildBuddyError.parse(e).description });
    } finally {
      this.setState({ editModalSubmitting: false });
    }

    await this.fetchIPRules();
  }

  private onEditModalCidrChanged(e: React.ChangeEvent<HTMLInputElement>) {
    const newValue = e.target.value;
    this.setState((prevState) => {
      return { editModalRule: iprules.IPRule.create({ ...prevState.editModalRule, cidr: newValue }) };
    });
  }

  private onEditModalDescriptionChanged(e: React.ChangeEvent<HTMLInputElement>) {
    const newValue = e.target.value;
    this.setState((prevState) => {
      return { editModalRule: iprules.IPRule.create({ ...prevState.editModalRule, description: newValue }) };
    });
  }

  private renderEditModal() {
    return (
      <Modal
        className="ip-rule-edit-modal"
        isOpen={this.state.editModalOpen}
        onRequestClose={this.onCloseEditModal.bind(this)}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>{this.state.editModalTitle}</DialogTitle>
          </DialogHeader>
          <form className="ip-rule-form" onSubmit={this.onSubmitEditModal.bind(this)}>
            <DialogBody>
              {this.state.editModalError && <div className="form-error">{this.state.editModalError}</div>}
              <div className="field-container">
                <label htmlFor="ip-range">IP or IP range</label>
                <TextInput
                  name="ip-range"
                  value={this.state.editModalRule.cidr}
                  onChange={this.onEditModalCidrChanged.bind(this)}
                  placeholder="e.g. 1.2.3.4 or 1.2.3.0/24"
                />
                <label htmlFor="description">Description</label>
                <TextInput
                  name="description"
                  value={this.state.editModalRule.description}
                  onChange={this.onEditModalDescriptionChanged.bind(this)}
                />
                <div className="propagation-note">Rule changes may take up to 5 minutes to propagate.</div>
              </div>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {this.state.editModalSubmitting && <Spinner />}
                <OutlinedButton type="button" onClick={this.onCloseEditModal.bind(this)}>
                  Cancel
                </OutlinedButton>
                <FilledButton type="submit" disabled={this.state.editModalSubmitting}>
                  {this.state.editModalSubmitLabel}
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </form>
        </Dialog>
      </Modal>
    );
  }

  private onCloseDeleteModal() {
    this.setState({
      deleteModalOpen: false,
      deleteModalSubmitting: false,
    });
  }

  private async onConfirmDeleteRule() {
    this.setState({ deleteModalSubmitting: true });

    try {
      await rpc_service.service.deleteIPRule(
        iprules.DeleteRuleRequest.create({ ipRuleId: this.state.deleteModalRule.ipRuleId })
      );
      this.onCloseDeleteModal();
    } catch (e) {
      error_service.handleError(e);
    } finally {
      this.setState({ deleteModalSubmitting: false });
    }

    await this.fetchIPRules();
  }

  private renderDeleteModal() {
    return (
      <Modal
        className="ip-rule-delete-modal"
        isOpen={this.state.deleteModalOpen}
        onRequestClose={this.onCloseDeleteModal.bind(this)}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>Confirm deletion</DialogTitle>
          </DialogHeader>
          <DialogBody>
            Are you sure you want to delete the IP rule{" "}
            <span className="delete-modal-rule">{this.state.deleteModalRule.cidr}</span>?
            <div className="propagation-note">Rule changes may take up to 5 minutes to propagate.</div>
          </DialogBody>
          <DialogFooter>
            <DialogFooterButtons>
              {this.state.deleteModalSubmitting && <Spinner />}
              <OutlinedButton disabled={this.state.deleteModalSubmitting} onClick={this.onCloseDeleteModal.bind(this)}>
                Cancel
              </OutlinedButton>
              <FilledButton
                className="destructive"
                disabled={this.state.deleteModalSubmitting}
                onClick={this.onConfirmDeleteRule.bind(this)}>
                Delete
              </FilledButton>
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }
  private onAddNewRule() {
    this.setState({
      editModalOpen: true,
      editModalTitle: "Add IP rule",
      editModalRule: iprules.IPRule.create(),
      editModalSubmitLabel: "Add",
    });
  }

  private onEditRule(rule: iprules.IPRule) {
    this.setState({
      editModalOpen: true,
      editModalTitle: "Edit IP rule",
      editModalRule: iprules.IPRule.create(rule),
      editModalSubmitLabel: "Update",
    });
  }

  private onDeleteRule(rule: iprules.IPRule) {
    this.setState({
      deleteModalOpen: true,
      deleteModalRule: rule,
    });
  }

  private onConfigureEnforcement(enable: boolean) {
    rpcService.service
      .setIPRulesConfig(iprules.SetRulesConfigRequest.create({ enforceIpRules: enable }))
      .then((r) => this.setState({ enforcementEnabled: enable }))
      .catch((e) => errorService.handleError(e));
  }

  private onCloseBulkAddModal() {
    this.setState({
      bulkModalOpen: false,
      bulkModalSubmitting: false,
      bulkModalText: "",
      bulkModalError: "",
    });
  }

  private onBulkAddRules() {
    this.setState({
      bulkModalSubmitting: true,
    });

    const existingRules = new Set<string>();
    for (const rule of this.state.rules) {
      existingRules.add(rule.cidr);
    }

    const newRules = new Array<iprules.IPRule>();
    for (const rule of this.state.bulkModalText.split(/[\s,]+/)) {
      if (existingRules.has(rule) || existingRules.has(rule + "/32") || existingRules.has(rule + "/128")) {
        console.log("skipping existing rule", rule);
        continue;
      }
      newRules.push(iprules.IPRule.create({ cidr: rule }));
      existingRules.add(rule);
    }

    const requests = new Array<Promise<iprules.AddRuleResponse>>();
    for (const rule of newRules) {
      requests.push(
        rpcService.service
          .addIPRule(iprules.AddRuleRequest.create({ rule: rule }))
          .catch((e) => Promise.reject(rule.cidr + ": " + BuildBuddyError.parse(e)))
      );
    }

    Promise.allSettled(requests).then((results) => {
      const errors = new Array<string>();
      for (const result of results) {
        if (result.status == "rejected") {
          errors.push(result.reason);
        }
      }
      this.setState({
        bulkModalSubmitting: false,
        bulkModalError: errors.join("\n"),
      });
      this.fetchIPRules();
    });
  }

  private onBulkAddEntryChanged(e: React.ChangeEvent<HTMLTextAreaElement>) {
    this.setState({
      bulkModalText: e.target.value,
    });
  }

  private onBulkAddDismissError() {
    this.setState({
      bulkModalError: "",
    });
  }

  private renderBulkAddModal() {
    return (
      <Modal
        className="ip-rule-bulk-add-modal"
        isOpen={this.state.bulkModalOpen}
        onRequestClose={this.onCloseBulkAddModal.bind(this)}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>Bulk add rules</DialogTitle>
          </DialogHeader>
          <DialogBody>
            {this.state.bulkModalError && (
              <div className="ip-rule-bulk-add-error">
                Some rules could not be added:
                <pre>{this.state.bulkModalError}</pre>
              </div>
            )}
            {!this.state.bulkModalError && (
              <>
                <div className="ip-rule-bulk-add-instructions">
                  Rules may be separated by either whitespace or commas.
                </div>
                <textarea
                  className="ip-rule-bulk-add-entry text-input"
                  onChange={this.onBulkAddEntryChanged.bind(this)}
                  value={this.state.bulkModalText}></textarea>
              </>
            )}
            <div className="propagation-note">Rule changes may take up to 5 minutes to propagate.</div>
          </DialogBody>
          <DialogFooter>
            <DialogFooterButtons>
              {this.state.bulkModalSubmitting && <Spinner />}
              <OutlinedButton disabled={this.state.bulkModalSubmitting} onClick={this.onCloseBulkAddModal.bind(this)}>
                Cancel
              </OutlinedButton>
              {this.state.bulkModalError && (
                <FilledButton onClick={this.onBulkAddDismissError.bind(this)}>Back</FilledButton>
              )}
              {!this.state.bulkModalError && (
                <FilledButton disabled={this.state.bulkModalSubmitting} onClick={this.onBulkAddRules.bind(this)}>
                  Add
                </FilledButton>
              )}
            </DialogFooterButtons>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }

  private onOpenBulkAddRulesModal() {
    this.setState({
      bulkModalOpen: true,
    });
  }

  render() {
    if (!this.props.user) return <></>;

    return (
      <div className="ip-rules">
        {this.renderEditModal()}
        {this.renderDeleteModal()}
        {this.renderBulkAddModal()}
        {!this.state.enforcementEnabled && (
          <div className="enforcement">
            <div className="enforcement-state">IP rules are NOT currently being enforced for this organization</div>
            <div>
              <FilledButton onClick={this.onConfigureEnforcement.bind(this, true)}>Enable</FilledButton>
            </div>
          </div>
        )}
        {this.state.enforcementEnabled && (
          <div className="enforcement">
            <div className="enforcement-state">IP rules are being enforced for this organization</div>
            <div>
              <FilledButton className="destructive" onClick={this.onConfigureEnforcement.bind(this, false)}>
                Disable
              </FilledButton>
            </div>
          </div>
        )}
        <div className="ip-rules-table">
          <FilledButton onClick={this.onAddNewRule.bind(this)} debug-id="create-new-ip-rule">
            Add rule
          </FilledButton>
          <OutlinedButton onClick={this.onOpenBulkAddRulesModal.bind(this)} className="bulk-add-button">
            Bulk add rules
          </OutlinedButton>
          <div className="ip-rules-list">
            {this.state.rules.map((rule) => (
              <div className="ip-rules-list-item">
                <div className="ip-rule-cidr">{rule.cidr}</div>
                <div className="ip-rule-description">{rule.description}</div>
                <OutlinedButton className="ip-rule-edit-button" onClick={this.onEditRule.bind(this, rule)}>
                  Edit
                </OutlinedButton>
                <OutlinedButton className="destructive" onClick={this.onDeleteRule.bind(this, rule)}>
                  Delete
                </OutlinedButton>
              </div>
            ))}
          </div>
          <div className="propagation-note">Rule changes may take up to 5 minutes to propagate.</div>
        </div>
      </div>
    );
  }
}
