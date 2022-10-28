import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import error_service from "../../../app/errors/error_service";
import rpc_service from "../../../app/service/rpc_service";
import { quota } from "../../../proto/quota_ts_proto";
import { durationMillis as formatDurationMillis } from "../../../app/format/format";
import { durationToMillis } from "../../../app/util/proto";
import alert_service from "../../../app/alert/alert_service";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import TextInput from "../../../app/components/input/input";
import Select, { Option } from "../../../app/components/select/select";
import LinkButton, { OutlinedLinkButton } from "../../../app/components/button/link_button";
import QuotaBreadcrumbs from "./quota_breadcrumbs";

const DEFAULT_BUCKET_NAME = "default";

export interface NamespaceProps {
  path: string;
  search: URLSearchParams;
}

interface State {
  loading?: boolean;
  response?: quota.IGetNamespaceResponse;

  // "Add member to bucket" dialog
  bucketToAssign?: string;
  memberToAdd?: string;
  assignLoading?: boolean;

  // Reassign bucket dialog
  quotaKeyToEdit?: { bucketName: string; key: string };
  quotaKeyNewBucket?: string;
  reassignLoading?: boolean;

  // Delete bucket dialog
  bucketToDelete?: string;
  deleteLoading?: boolean;
}

/**
 * Lists all the buckets in a namespace and allows adding, editing, and removing
 * buckets.
 */
export default class NamespaceComponent extends React.Component<NamespaceProps, State> {
  state: State = {};

  componentDidMount() {
    this.fetch();
  }

  private getNamespaceName() {
    return this.props.search.get("name") || "";
  }

  private fetch() {
    this.setState({ loading: true });
    rpc_service.service
      .getNamespace({ namespace: this.getNamespaceName() })
      .then((response) => {
        this.setState({ response });
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onClickAssign(bucketName: string) {
    this.setState({ bucketToAssign: bucketName });
  }
  private onCloseAssignModal() {
    this.setState({ bucketToAssign: "" });
  }
  private onChangeMemberToAdd(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ memberToAdd: e.target.value });
  }
  private onSubmitAssign() {
    const bucketName = this.state.bucketToAssign;
    const namespace = this.getNamespaceName();
    const member = this.state.memberToAdd || "";
    this.setState({ assignLoading: true });
    rpc_service.service
      .applyBucket({
        namespace,
        bucketName,
        key: quotaKeyFromString(member),
      })
      .then(() => {
        this.setState({ bucketToAssign: undefined, memberToAdd: undefined });
        alert_service.success(`Bucket "${bucketName}" applied to "${member}" successfully.`);
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ assignLoading: false }));
  }

  private onClickEditQuotaKey(bucketName: string, key: string) {
    this.setState({ quotaKeyToEdit: { bucketName, key } });
  }
  private onCloseReassignModal() {
    this.setState({ quotaKeyToEdit: undefined });
  }
  private onClickSubmitReassign() {
    this.setState({ reassignLoading: true });
    const newBucket = this.state.quotaKeyNewBucket;
    const oldBucket = this.state.quotaKeyToEdit?.bucketName;
    const member = this.state.quotaKeyToEdit?.key;
    rpc_service.service
      .applyBucket({
        namespace: this.getNamespaceName(),
        bucketName: newBucket,
        key: quotaKeyFromString(member || ""),
      })
      .then(() => {
        this.setState({ quotaKeyToEdit: undefined, quotaKeyNewBucket: undefined });
        alert_service.success(`Successfully re-assigned "${member}" from "${oldBucket}" to "${newBucket}"`);
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ reassignLoading: false }));
  }
  private onChangeBucketForReassign(e: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ quotaKeyNewBucket: e.target.value });
  }

  private getReturnURL(): string {
    return `${this.props.path}?${this.props.search}`;
  }

  private getAddBucketURL(): string {
    const search = new URLSearchParams({
      namespace: this.getNamespaceName(),
      return: this.getReturnURL(),
    });
    return `/settings/server/quota/bucket?${search}`;
  }
  private getEditBucketURL(bucket: quota.IBucket): string {
    const search = new URLSearchParams({
      namespace: this.getNamespaceName(),
      bucket: bucket.name,
      initialValues: JSON.stringify(bucket),
      return: this.getReturnURL(),
    });
    return `/settings/server/quota/bucket?${search}`;
  }

  private onClickDelete(bucketName: string) {
    this.setState({ bucketToDelete: bucketName });
  }
  private onCloseDeleteDialog() {
    this.setState({ bucketToDelete: undefined });
  }
  private onConfirmDelete() {
    this.setState({ deleteLoading: true });
    const bucketName = this.state.bucketToDelete;
    rpc_service.service
      .modifyNamespace({ namespace: this.getNamespaceName(), removeBucket: bucketName })
      .then(() => {
        this.setState({ bucketToDelete: undefined });
        alert_service.success(`Deleted bucket "${bucketName}" from namespace "${this.getNamespaceName()}"`);
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ deleteLoading: false }));
  }

  render() {
    if (this.state.loading) return <div className="loading" />;
    if (!this.state.response) return null;
    return (
      <div className="quota-namespace quota-column-layout">
        <QuotaBreadcrumbs
          items={[{ label: "Quota namespaces", href: "/settings/server/quota" }, { label: this.getNamespaceName() }]}
        />
        <div>
          <LinkButton className="big-button" href={this.getAddBucketURL()}>
            Add bucket
          </LinkButton>
        </div>
        <div className="bucket-list quota-column-layout">
          {!this.state.response.namespaces[0] && <div>This namespace has no associated buckets.</div>}
          {this.state.response.namespaces[0]?.assignedBuckets.sort(compareBuckets).map((assigned) => (
            <div className="bucket">
              <div className="bucket-info">
                <span className="bucket-details">
                  Bucket <b>{assigned.bucket.name}</b>: {assigned.bucket.maxRate.numRequests} requests per{" "}
                  {formatDurationMillis(durationToMillis(assigned.bucket.maxRate.period))} &bull;{" "}
                  {assigned.bucket.maxBurst} max burst
                </span>
                <FilledButton onClick={this.onClickAssign.bind(this, assigned.bucket.name)}>Assign</FilledButton>
                <OutlinedLinkButton href={this.getEditBucketURL(assigned.bucket)}>Edit</OutlinedLinkButton>
                <OutlinedButton className="destructive" onClick={this.onClickDelete.bind(this, assigned.bucket.name)}>
                  Delete
                </OutlinedButton>
              </div>
              <div className="bucket-members-list">
                {assigned.quotaKeys?.length === 0 && assigned.bucket.name !== DEFAULT_BUCKET_NAME && (
                  <div className="bucket-empty-message">No users are assigned to this bucket.</div>
                )}
                {assigned.quotaKeys?.length === 0 && assigned.bucket.name === DEFAULT_BUCKET_NAME && (
                  <div className="bucket-empty-message">Users in the default bucket are not shown.</div>
                )}
                {assigned.quotaKeys.map((key) => (
                  <div className="bucket-member-item">
                    <span className="bucket-member-key">{key}</span>
                    <OutlinedButton onClick={this.onClickEditQuotaKey.bind(this, assigned.bucket.name, key)}>
                      Edit
                    </OutlinedButton>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
        <SimpleModalDialog
          title="Confirm deletion"
          isOpen={Boolean(this.state.bucketToDelete)}
          submitLabel="Delete"
          onRequestClose={this.onCloseDeleteDialog.bind(this)}
          onSubmit={this.onConfirmDelete.bind(this)}
          loading={this.state.deleteLoading}
          destructive>
          Remove bucket <b>{this.state.bucketToDelete}</b> from namespace <b>{this.getNamespaceName()}</b> and delete
          all assigned members of this bucket? This cannot be undone.
        </SimpleModalDialog>
        <SimpleModalDialog
          title="Assign to bucket"
          isOpen={Boolean(this.state.bucketToAssign)}
          submitLabel="Assign"
          onRequestClose={this.onCloseAssignModal.bind(this)}
          onSubmit={this.onSubmitAssign.bind(this)}
          loading={this.state.assignLoading}
          submitDisabled={!this.state.memberToAdd}>
          <div>IP address or group ID</div>
          <TextInput onChange={this.onChangeMemberToAdd.bind(this)} value={this.state.memberToAdd} />
        </SimpleModalDialog>
        <SimpleModalDialog
          title="Reassign bucket member"
          isOpen={Boolean(this.state.quotaKeyToEdit)}
          submitLabel="Assign"
          onRequestClose={this.onCloseReassignModal.bind(this)}
          onSubmit={this.onClickSubmitReassign.bind(this)}
          loading={this.state.reassignLoading}
          submitDisabled={
            !this.state.quotaKeyNewBucket || this.state.quotaKeyNewBucket === this.state.quotaKeyToEdit?.bucketName
          }>
          <div>
            Re-assign <b>{this.state.quotaKeyToEdit?.key}</b> from <b>{this.state.quotaKeyToEdit?.bucketName}</b> to:
          </div>
          <Select
            value={this.state.quotaKeyNewBucket || this.state.quotaKeyToEdit?.bucketName}
            onChange={this.onChangeBucketForReassign.bind(this)}>
            <Option value={DEFAULT_BUCKET_NAME}>default (unassigned)</Option>
            {this.state.response.namespaces[0]?.assignedBuckets
              .sort(compareBuckets)
              .map((assigned) => assigned.bucket.name)
              .filter((name) => name !== DEFAULT_BUCKET_NAME)
              .map((name) => (
                <Option value={name}>{name}</Option>
              ))}
          </Select>
        </SimpleModalDialog>
      </div>
    );
  }
}

function quotaKeyFromString(val: string): quota.IQuotaKey {
  return val.includes(".") || val.includes(":") ? { ipAddress: val } : { groupId: val };
}

function compareBuckets(a: quota.IAssignedBucket, b: quota.IAssignedBucket): number {
  // Always show the default bucket first.
  if (a.bucket.name === "default") return -1;
  return a.bucket.name < b.bucket.name ? -1 : 1;
}
