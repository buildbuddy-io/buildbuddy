import { usecToTimestamp } from "../../../app/util/proto";
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

describe("usage bill model", () => {
  it("describes known products in their priced units", () => {
    const hits = new usage.BillLineItem({ name: "action_cache_hits", quantity: 27.096, unitPriceCents: 1200 });
    expect(productLabel(hits)).toBe("Action cache hits");
    expect(productQuantity(hits)).toBe("27,096 hits");
    expect(productPrice(hits)).toBe("$12 per 1,000 hits");

    const upload = new usage.BillLineItem({ name: "upload_overage_tb", quantity: 0.0138, unitPriceCents: 987654 });
    expect(productLabel(upload)).toBe("Cache upload overage");
    expect(productQuantity(upload)).toBe("13.8GB");
    expect(productPrice(upload)).toBe("$9,876.54 per TB");

    const cpu = new usage.BillLineItem({ name: "cpu_minutes", quantity: 42.4, unitPriceCents: 250 });
    expect(productQuantity(cpu)).toBe("42 min");
    expect(productPrice(cpu)).toBe("$2.50 per minute");
  });

  it("falls back to the raw product name", () => {
    const item = new usage.BillLineItem({ name: "new_product", quantity: 3, unitPriceCents: 150 });
    expect(productLabel(item)).toBe("new_product");
    expect(productQuantity(item)).toBe("3");
    expect(productPrice(item)).toBe("$1.50 per unit");
  });

  it("reports the allowance behind an overage from the unpriced lines", () => {
    const overage = new usage.BillLineItem({ name: "upload_overage_tb", quantity: 0.0142, unitPriceCents: 987654 });
    const hits = new usage.BillLineItem({ name: "action_cache_hits", quantity: 51.496, unitPriceCents: 1200 });
    const bill = new usage.Bill({
      lineItems: [
        hits,
        overage,
        new usage.BillLineItem({ name: "upload_used_tb", quantity: 0.0373 }),
        new usage.BillLineItem({ name: "upload_included_tb", quantity: 0.0231 }),
      ],
    });
    expect(billedLineItems(bill)).toEqual([hits, overage]);
    expect(allowance(bill, overage)).toEqual({
      label: "Total bytes uploaded to cache",
      used: "37.3GB",
      included: "23.1GB",
    });
    expect(allowance(bill, hits)).toBeNull();
    expect(allowance(new usage.Bill({ lineItems: [overage] }), overage)).toBeNull();
  });

  it("detects lines that cover part of the period", () => {
    const seconds = (s: number) => usecToTimestamp(s * 1e6);
    const bill = new usage.Bill({ periodStart: seconds(1000), periodEnd: seconds(5000) });
    const line = (start: number, end: number) =>
      new usage.BillLineItem({ periodStart: seconds(start), periodEnd: seconds(end) });
    expect(isPartialPeriod(bill, line(1000, 5000))).toBe(false);
    expect(isPartialPeriod(bill, line(1000, 3000))).toBe(true);
    expect(isPartialPeriod(bill, line(3000, 5000))).toBe(true);
    expect(isPartialPeriod(bill, new usage.BillLineItem({}))).toBe(false);
  });

  it("reports how much of the credit is used", () => {
    expect(creditPercent(0, 1000)).toBe(0);
    expect(creditPercent(250, 1000)).toBe(25);
    expect(creditPercent(1500, 1000)).toBe(100);
    expect(creditPercent(100, 0)).toBe(0);

    expect(creditLevel(250, 1000)).toBe("ok");
    expect(creditLevel(750, 1000)).toBe("low");
    expect(creditLevel(1000, 1000)).toBe("used-up");
  });
});
