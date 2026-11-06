import { bytes, formatPrice, formatWithCommas } from "../../../app/format/format";
import { usage } from "../../../proto/usage_ts_proto";

interface ProductDisplay {
  label: string;
  /** Formats the quantity, which is in the product's priced unit. */
  formatQuantity: (quantity: number) => string;
  priceUnit: string;
  /**
   * For an overage: the unpriced products that report the usage it is measured
   * from and the amount included, labeled like the matching usage page field.
   */
  allowance?: { label: string; usedProduct: string; includedProduct: string };
}

/** Keyed by the Metronome product name. */
const PRODUCTS: Record<string, ProductDisplay> = {
  action_cache_hits: {
    label: "Action cache hits",
    formatQuantity: (q) => `${formatWithCommas(Math.round(q * 1000))} hits`,
    priceUnit: "per 1,000 hits",
  },
  cpu_minutes: {
    label: "Remote execution CPU",
    formatQuantity: (q) => `${formatWithCommas(Math.round(q))} min`,
    priceUnit: "per minute",
  },
  upload_overage_tb: {
    label: "Cache upload overage",
    formatQuantity: (q) => bytes(q * 1e12),
    priceUnit: "per TB",
    allowance: {
      label: "Total bytes uploaded to cache",
      usedProduct: "upload_used_tb",
      includedProduct: "upload_included_tb",
    },
  },
  external_download_overage_tb: {
    label: "External download overage",
    formatQuantity: (q) => bytes(q * 1e12),
    priceUnit: "per TB",
    allowance: {
      label: "External downloads",
      usedProduct: "external_download_used_tb",
      includedProduct: "external_download_included_tb",
    },
  },
};

const ALLOWANCE_PRODUCTS = new Set(
  Object.values(PRODUCTS).flatMap((p) => (p.allowance ? [p.allowance.usedProduct, p.allowance.includedProduct] : []))
);

/** The lines that are charged for. The rest only report usage and what is included. */
export function billedLineItems(bill: usage.IBill): usage.IBillLineItem[] {
  return (bill.lineItems ?? []).filter((item) => !ALLOWANCE_PRODUCTS.has(item.name ?? ""));
}

/** For an overage line: the usage it is measured from and how much of it is included. */
export function allowance(
  bill: usage.IBill,
  item: usage.IBillLineItem
): { label: string; used: string; included: string } | null {
  const a = PRODUCTS[item.name ?? ""]?.allowance;
  if (!a) return null;
  const quantity = (product: string) => (bill.lineItems ?? []).find((l) => l.name === product)?.quantity;
  const used = quantity(a.usedProduct);
  const included = quantity(a.includedProduct);
  if (used == null || included == null) return null;
  return { label: a.label, used: bytes(used * 1e12), included: bytes(included * 1e12) };
}

export function productLabel(item: usage.IBillLineItem): string {
  return PRODUCTS[item.name ?? ""]?.label ?? item.name ?? "";
}

export function productQuantity(item: usage.IBillLineItem): string {
  const quantity = item.quantity ?? 0;
  const product = PRODUCTS[item.name ?? ""];
  return product ? product.formatQuantity(quantity) : formatWithCommas(quantity);
}

export function productPrice(item: usage.IBillLineItem): string {
  const price = formatPrice(item.unitPriceCents ?? 0);
  const product = PRODUCTS[item.name ?? ""];
  return product ? `${price} ${product.priceUnit}` : `${price} per unit`;
}

/** Whether a line covers only part of the billing period, as it does after a price change. */
export function isPartialPeriod(bill: usage.IBill, item: usage.IBillLineItem): boolean {
  if (!item.periodStart || !item.periodEnd) return false;
  return (
    +(item.periodStart.seconds ?? 0) !== +(bill.periodStart?.seconds ?? 0) ||
    +(item.periodEnd.seconds ?? 0) !== +(bill.periodEnd?.seconds ?? 0)
  );
}

/** The share of the free credit that is used, from 0 to 100. */
export function creditPercent(usedCents: number, grantedCents: number): number {
  if (grantedCents <= 0) return 0;
  return Math.min(100, Math.max(0, (usedCents / grantedCents) * 100));
}

export type CreditLevel = "ok" | "low" | "used-up";

export function creditLevel(usedCents: number, grantedCents: number): CreditLevel {
  const percent = creditPercent(usedCents, grantedCents);
  if (percent >= 100) return "used-up";
  if (percent >= 75) return "low";
  return "ok";
}
