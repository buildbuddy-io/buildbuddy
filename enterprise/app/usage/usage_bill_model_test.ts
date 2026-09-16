import { usage } from "../../../proto/usage_ts_proto";
import {
  formatCents,
  formatPrice,
  productFootnote,
  productLabel,
  productPrice,
  productQuantity,
} from "./usage_bill_model";

describe("usage bill model", () => {
  it("formats cents as dollars", () => {
    expect(formatCents(98765.4)).toBe("$987.65");
    expect(formatCents(0)).toBe("$0.00");
    expect(formatCents(123456)).toBe("$1,234.56");
  });

  it("formats unit prices without trailing zeros", () => {
    expect(formatPrice(12)).toBe("$0.12");
    expect(formatPrice(7)).toBe("$0.07");
    expect(formatPrice(300000)).toBe("$3,000");
  });

  it("describes known products in their priced units", () => {
    const hits = new usage.BillLineItem({ name: "action_cache_hits", quantity: 27.096, unitPriceCents: 1200 });
    expect(productLabel(hits)).toBe("Action cache hits");
    expect(productFootnote(hits)).toBe("");
    expect(productQuantity(hits)).toBe("27,096 hits");
    expect(productPrice(hits)).toBe("$12 per 1,000 hits");

    const upload = new usage.BillLineItem({ name: "upload_overage_tb", quantity: 0.0138, unitPriceCents: 987654 });
    expect(productLabel(upload)).toBe("Cache upload overage");
    expect(productFootnote(upload)).toBe("**");
    expect(productQuantity(upload)).toBe("13.8GB");
    expect(productPrice(upload)).toBe("$9,876.54 per TB");

    const cpu = new usage.BillLineItem({ name: "cpu_minutes", quantity: 42.4, unitPriceCents: 250 });
    expect(productFootnote(cpu)).toBe("*");
    expect(productQuantity(cpu)).toBe("42 min");
    expect(productPrice(cpu)).toBe("$2.50 per minute");
  });

  it("falls back to the raw product name", () => {
    const item = new usage.BillLineItem({ name: "new_product", quantity: 3, unitPriceCents: 150 });
    expect(productLabel(item)).toBe("new_product");
    expect(productQuantity(item)).toBe("3");
    expect(productPrice(item)).toBe("$1.50 per unit");
  });
});
