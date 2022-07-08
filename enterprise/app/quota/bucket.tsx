import React from "react";
import alert_service from "../../../app/alert/alert_service";
import error_service from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpc_service from "../../../app/service/rpc_service";
import { quota } from "../../../proto/quota_ts_proto";
import BucketForm, { BucketFormValues } from "./bucket_form";
import QuotaBreadcrumbs, { BreadcrumbItem } from "./quota_breadcrumbs";

interface BucketProps {
  search: URLSearchParams;
}

/**
 * Page that renders a form to create or edit a bucket.
 *
 * Respects the following URL params:
 * - `namespace`: optional namespace to add the bucket to. If editing a bucket,
 *   this is the namespace of the bucket being edited.
 * - `bucket`: optional name of an existing bucket being edited. If this is
 *   specified then `namespace` should also be specified.
 * - `return`: optional URL to be redirected back to when the form is submitted.
 */
export default class BucketComponent extends React.Component<BucketProps> {
  private onSubmit(values: BucketFormValues): Promise<any> {
    return rpc_service.service
      .modifyNamespace({
        namespace: this.props.search.get("namespace") || values.namespace,
        ...(this.props.search.get("bucket") ? { updateBucket: values.bucket } : { addBucket: values.bucket }),
      })
      .then(() => {
        alert_service.success("Bucket saved successfully!");
        router.navigateTo(this.props.search.get("return") || "/settings/server/quota");
      })
      .catch((e) => error_service.handleError(e));
  }

  private getSubmitLabel() {
    if (this.props.search.get("bucket")) return "Save";
    if (this.props.search.get("namespace")) return "Add";
    return "Create";
  }

  private getInitialFormValues() {
    const serializedBucket = this.props.search.get("initialValues");
    return {
      namespace: this.props.search.get("namespace") || undefined,
      bucket: serializedBucket ? (JSON.parse(serializedBucket) as quota.IBucket) : undefined,
    };
  }

  private getBreadcrumbItems(): BreadcrumbItem[] {
    const items: BreadcrumbItem[] = [];
    items.push({ label: "Quota namespaces", href: "/settings/server/quota" });
    const namespace = this.props.search.get("namespace");
    if (!namespace) {
      items.push({ label: "Create" });
      return items;
    }
    items.push({
      label: namespace,
      href: `/settings/server/quota/namespace?name=${encodeURIComponent(namespace)}`,
    });
    const bucket = this.props.search.get("bucket");
    if (!bucket) {
      items.push({ label: "Add bucket" });
      return items;
    }
    items.push({ label: bucket });
    return items;
  }

  render() {
    return (
      <div className="quota-bucket quota-column-layout">
        <QuotaBreadcrumbs items={this.getBreadcrumbItems()} />
        {!this.props.search.get("namespace") && (
          <div>
            A namespace specifies a name of a resource that should be quota-controlled. Users of that resource can be
            assigned to one or more buckets. To create a namespace, create the first bucket for it.
          </div>
        )}
        <div>
          <BucketForm
            namespaceEditable={!this.props.search.get("namespace")}
            nameEditable={!this.props.search.get("bucket")}
            submitLabel={this.getSubmitLabel()}
            onSubmit={this.onSubmit.bind(this)}
            initialValues={this.getInitialFormValues()}
          />
        </div>
      </div>
    );
  }
}
