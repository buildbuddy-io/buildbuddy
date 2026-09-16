import { bytes, formatWithCommas } from "../../../app/format/format";
import { usage } from "../../../proto/usage_ts_proto";

/** Footnotes shown under the bill, in the order they are referenced. */
export const FOOTNOTES = [
  { marker: "*", text: "Each CPU minute includes 100 MB of upload, and the first 1,000 minutes each month are free." },
  { marker: "**", text: "Every 1,000 action cache hits include 250 MB of upload and 500 MB of external download." },
];

interface ProductDisplay {
  label: string;
  /** Formats the quantity, which is in the product's priced unit. */
  formatQuantity: (quantity: number) => string;
  priceUnit: string;
  /** Index into FOOTNOTES. */
  footnote?: number;
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
    footnote: 0,
  },
  upload_overage_tb: {
    label: "Cache upload overage",
    formatQuantity: (q) => bytes(q * 1e12),
    priceUnit: "per TB",
    footnote: 1,
  },
  external_download_overage_tb: {
    label: "External download overage",
    formatQuantity: (q) => bytes(q * 1e12),
    priceUnit: "per TB",
    footnote: 1,
  },
};

/** Formats an amount in US cents as dollars, e.g. 815.84 -> "$8.16". */
export function formatCents(cents: number): string {
  return (cents / 100).toLocaleString("en-US", { style: "currency", currency: "USD" });
}

/** Formats a unit price in US cents, e.g. 10000 -> "$100", 25 -> "$0.25". */
export function formatPrice(cents: number): string {
  const dollars = cents / 100;
  const fractionDigits = Number.isInteger(dollars) ? 0 : 2;
  return dollars.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}

export function productLabel(item: usage.IBillLineItem): string {
  return PRODUCTS[item.name ?? ""]?.label ?? item.name ?? "";
}

/** The footnote marker for the product, or "" if it has none. */
export function productFootnote(item: usage.IBillLineItem): string {
  const footnote = PRODUCTS[item.name ?? ""]?.footnote;
  return footnote === undefined ? "" : FOOTNOTES[footnote].marker;
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
