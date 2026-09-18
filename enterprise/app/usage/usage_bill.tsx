import moment from "moment";
import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";
import {
  FOOTNOTES,
  formatCents,
  productFootnote,
  productLabel,
  productPrice,
  productQuantity,
} from "./usage_bill_model";

interface State {
  bill?: usage.IBill | null;
}

/** Shows the group's cost so far this billing period, if it is billed for usage. */
export default class UsageBillCard extends React.Component<{}, State> {
  state: State = {};

  componentDidMount() {
    rpcService.service
      .getCurrentBill(new usage.GetCurrentBillRequest())
      .then((response) => this.setState({ bill: response.bill ?? null }))
      .catch((e) => {
        console.warn("Failed to load current bill", e);
        this.setState({ bill: null });
      });
  }

  render() {
    const bill = this.state.bill;
    if (!bill) {
      return null;
    }
    const start = moment.unix(+(bill.periodStart?.seconds ?? 0)).utc();
    // The period end is exclusive, so show the last day it covers.
    const end = moment
      .unix(+(bill.periodEnd?.seconds ?? 0))
      .utc()
      .subtract(1, "day");
    const fetched = moment.unix(+(bill.fetchedAt?.seconds ?? 0));
    return (
      <div className="card usage-card usage-bill-card">
        <div className="content">
          <div className="usage-bill-header">
            <div>
              <div className="usage-bill-title">Current bill</div>
              <div className="usage-bill-subtitle">
                {start.format("MMM D")} – {end.format("MMM D, YYYY")} (UTC) · updated {fetched.fromNow()}
              </div>
            </div>
            <div className="usage-bill-total">{formatCents(bill.totalCents ?? 0)}</div>
          </div>
          <div className="usage-bill-table">
            <div className="usage-bill-heading">Product</div>
            <div className="usage-bill-heading usage-bill-number">Billable usage</div>
            <div className="usage-bill-heading usage-bill-number">Price</div>
            <div className="usage-bill-heading usage-bill-number">Cost</div>
            {(bill.lineItems ?? []).map((item) => (
              <React.Fragment key={item.name}>
                <div className="usage-bill-product">
                  {productLabel(item)}
                  {productFootnote(item)}
                </div>
                <div className="usage-bill-number">{productQuantity(item)}</div>
                <div className="usage-bill-number">{productPrice(item)}</div>
                <div className="usage-bill-number usage-bill-cost">{formatCents(item.totalCents ?? 0)}</div>
              </React.Fragment>
            ))}
          </div>
          <div className="usage-bill-notes">
            {FOOTNOTES.map((footnote) => (
              <div key={footnote.marker}>
                {footnote.marker} {footnote.text}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
