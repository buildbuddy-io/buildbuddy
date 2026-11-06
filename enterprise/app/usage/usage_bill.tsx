import moment from "moment";
import React from "react";
import { formatCents } from "../../../app/format/format";
import { usage } from "../../../proto/usage_ts_proto";
import {
  allowance,
  billedLineItems,
  creditLevel,
  creditPercent,
  isPartialPeriod,
  productLabel,
  productPrice,
  productQuantity,
} from "./usage_bill_model";

interface Props {
  bill: usage.IBill;
  /** The usage page's period header, shown at the top of the card. */
  periodHeader: React.ReactNode;
}

/** Formats a period with an exclusive end as its first and last day, e.g. "Sep 1 – Sep 18". */
function formatDays(startSeconds: number, endSeconds: number): string {
  const start = moment.unix(startSeconds).utc();
  const end = moment.unix(endSeconds).utc().subtract(1, "day");
  return `${start.format("MMM D")} – ${end.format("MMM D")}`;
}

/** Shows the group's cost so far this billing period. */
export default class UsageBillCard extends React.Component<Props> {
  private renderAllowance(bill: usage.IBill, item: usage.IBillLineItem) {
    // The allowance is for the whole period, so it does not belong to one price's line.
    const a = isPartialPeriod(bill, item) ? null : allowance(bill, item);
    if (!a) return null;
    return (
      <>
        <div className="usage-bill-allowance">{a.label}</div>
        <div className="usage-bill-allowance usage-bill-allowance-value">{a.used}</div>
        <div className="usage-bill-allowance">Included</div>
        <div className="usage-bill-allowance usage-bill-allowance-value">{a.included}</div>
      </>
    );
  }

  render() {
    const bill = this.props.bill;
    const start = moment.unix(+(bill.periodStart?.seconds ?? 0)).utc();
    // The period end is exclusive, so show the last day it covers.
    const end = moment
      .unix(+(bill.periodEnd?.seconds ?? 0))
      .utc()
      .subtract(1, "day");
    const fetched = moment.unix(+(bill.fetchedAt?.seconds ?? 0));
    const creditGranted = bill.creditGrantedCents ?? 0;
    const creditUsed = bill.creditUsedCents ?? 0;
    return (
      <div className="card usage-card usage-bill-card">
        <div className="content">
          {this.props.periodHeader}
          <div className="usage-bill-header">
            <div>
              <div className="usage-bill-title">Current bill</div>
              <div className="usage-bill-subtitle">
                {start.format("MMM D")} – {end.format("MMM D, YYYY")} (UTC) · updated {fetched.fromNow()} · all prices
                in USD
              </div>
            </div>
            <div className="usage-bill-total">{formatCents(bill.totalCents ?? 0)}</div>
          </div>
          {creditGranted > 0 && (
            <div className="usage-bill-credit">
              <div className="usage-bill-credit-label">
                <span>Free monthly credit</span>
                <span>
                  {formatCents(creditUsed)} of {formatCents(creditGranted)} used
                </span>
              </div>
              <div
                className={`usage-bill-meter ${creditLevel(creditUsed, creditGranted)}`}
                role="meter"
                aria-label="Free monthly credit used"
                aria-valuemin={0}
                aria-valuemax={creditGranted}
                aria-valuenow={Math.min(creditUsed, creditGranted)}>
                <div
                  className="usage-bill-meter-fill"
                  style={{ width: `${creditPercent(creditUsed, creditGranted)}%` }}
                />
              </div>
            </div>
          )}
          <div className="usage-bill-table">
            <div className="usage-bill-heading">Product</div>
            <div className="usage-bill-heading">Billable usage</div>
            <div className="usage-bill-heading">Price</div>
            <div className="usage-bill-heading usage-bill-cost">Cost</div>
            {billedLineItems(bill).map((item, index) => (
              <React.Fragment key={index}>
                <div className="usage-bill-product">
                  {productLabel(item)}
                  {isPartialPeriod(bill, item) && (
                    <span className="usage-bill-line-period">
                      {formatDays(+(item.periodStart?.seconds ?? 0), +(item.periodEnd?.seconds ?? 0))}
                    </span>
                  )}
                </div>
                <div>{productQuantity(item)}</div>
                <div>{productPrice(item)}</div>
                <div className="usage-bill-cost">{formatCents(item.totalCents ?? 0)}</div>
                {this.renderAllowance(bill, item)}
              </React.Fragment>
            ))}
            {creditUsed > 0 && (
              <>
                <div className="usage-bill-summary-label">Free monthly credit</div>
                <div className="usage-bill-cost">{formatCents(-creditUsed)}</div>
              </>
            )}
            <div className="usage-bill-summary-label usage-bill-total-row">Total</div>
            <div className="usage-bill-cost usage-bill-total-row">{formatCents(bill.totalCents ?? 0)}</div>
          </div>
        </div>
      </div>
    );
  }
}
