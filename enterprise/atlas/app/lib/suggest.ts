import { Completion } from "../../../../app/components/search_box/completion";
import { atlas } from "../../../../proto/atlas_ts_proto";
import { config } from "./config";
import rpcService from "./rpc_service";

const VALUES_MAX_AGE_MS = 5000;
const FETCH_LIMIT = 500;

/** The filter keys the server's query grammar knows, as typed: "kind:", "ns:", ... */
const keys: Completion[] = config.filterKeys.map((k) => ({ text: k.key + ":", detail: k.hint }));
const fields = new Set(config.filterKeys.map((k) => k.key));

/** Gets completion options for the token under the caret via backend RPC. */
export async function complete(typed: string): Promise<Completion[]> {
  const colon = typed.indexOf(":");
  // If there's no ":" then complete using the static list of field names.
  if (colon < 0) {
    const lower = typed.toLowerCase();
    return keys.filter((k) => k.text.startsWith(lower));
  }
  const field = typed.slice(0, colon).toLowerCase();
  if (!fields.has(field)) return [];
  let rest = typed.slice(colon + 1);
  let key = "";
  // Label is special-cased since completion may be applied either on label
  // key or value depending on context.
  if (field === "label") {
    const eq = rest.indexOf("=");
    // No equal sign means we are completing the label keys.
    if (eq < 0) return labelKeys(rest);
    if (eq === 0) return [];
    // Otherwise we are completing label values.
    key = rest.slice(0, eq);
    rest = rest.slice(eq + 1);
  }
  const lead = key ? `${field}:${key}=` : `${field}:`;
  return (await values(field, key, rest))
    .filter(startsWith(rest))
    .map((v) => ({ text: lead + v.value, count: v.count }));
}

async function labelKeys(typed: string): Promise<Completion[]> {
  const matches = (await values("label", "", typed)).filter(startsWith(typed));
  const lower = typed.toLowerCase();
  const exact = matches.find((k) => k.value.toLowerCase() === lower);
  const partialMatches: Completion[] = matches
    .filter((k) => k !== exact)
    .map((k) => ({ text: `label:${k.value}`, count: k.count, partial: true }));
  // If the token does not exactly match one of the label keys, return all the partial completion options.
  if (!exact) return partialMatches;
  // If the token has an exact match for a label key, show completion option for both the partial key matches and the
  // values for the exact match.
  const vals = await values("label", exact.value, "");
  return [...vals.map((v) => ({ text: `label:${exact.value}=${v.value}`, count: v.count })), ...partialMatches];
}

function startsWith(prefix: string): (v: atlas.FilterValue) => boolean {
  const lower = prefix.toLowerCase();
  return (v) => v.value.toLowerCase().startsWith(lower);
}

// Short-lived cache of field -> values.
const cache = new Map<string, { at: number; result: Promise<atlas.FilterValue[]> }>();

/** Fetches completions via a backend RPC (may be served from locally cached data if data was recently fetched). */
async function values(field: string, key: string, prefix: string): Promise<atlas.FilterValue[]> {
  // We're fetching label values which can be high-cardinality.
  // Always call through to the backend and let it do the filtering.
  if (key !== "") return request(field, key, prefix);
  // Otherwise try to use the local cache.
  let entry = cache.get(field);
  if (!entry || Date.now() - entry.at > VALUES_MAX_AGE_MS) {
    // No cache, make an RPC and cache the results.
    entry = { at: Date.now(), result: request(field, "", "") };
    cache.set(field, entry);
    const failed = entry;
    entry.result.catch(() => {
      if (cache.get(field) === failed) cache.delete(field);
    });
  }
  const all = await entry.result;
  if (all.length >= FETCH_LIMIT && prefix) return request(field, "", prefix);
  return all;
}

async function request(field: string, key: string, prefix: string): Promise<atlas.FilterValue[]> {
  const rsp = await rpcService.service.getFilterValues(
    new atlas.GetFilterValuesRequest({ field, key, prefix, limit: FETCH_LIMIT })
  );
  return rsp.values;
}
