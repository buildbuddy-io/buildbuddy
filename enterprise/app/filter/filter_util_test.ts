import capabilities from "../../../app/capabilities/capabilities";
import { AGENT_PARAM_NAME } from "../../../app/router/router_params";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { getProtoFilterParams, isAnyNonDateFilterSet } from "./filter_util";

describe("Agent filter URL to query", () => {
  let previousTagsUiEnabled: boolean;
  let previousAgentFilterEnabled: boolean;

  beforeEach(() => {
    previousTagsUiEnabled = capabilities.config.tagsUiEnabled;
    previousAgentFilterEnabled = capabilities.config.agentFilterEnabled;
    capabilities.config.tagsUiEnabled = true;
    capabilities.config.agentFilterEnabled = true;
  });

  afterEach(() => {
    capabilities.config.tagsUiEnabled = previousTagsUiEnabled;
    capabilities.config.agentFilterEnabled = previousAgentFilterEnabled;
  });

  it("keeps role, user tags and advanced filters independent of the coding agent string", () => {
    const search = new URLSearchParams({
      role: "CI",
      agent: "codex",
      tag: "coverage",
      sq: "tag:release",
    });

    const query = getProtoFilterParams(search);

    expect(query.role).toEqual(["CI"]);
    expect(query.tags).toEqual(["coverage"]);
    expect(query.genericFilters?.length).toBe(2);
    expect(query.genericFilters?.[0].value?.stringValue).toEqual(["release"]);
    expect(query.genericFilters?.[1].type).toBe(stat_filter.FilterType.AGENT_FILTER_TYPE);
    expect(query.genericFilters?.[1].operand).toBe(stat_filter.FilterOperand.IN_OPERAND);
    expect(query.genericFilters?.[1].value?.stringValue).toEqual(["codex"]);
    expect(query.genericFilters?.[1].negate).toBe(false);
    expect(isAnyNonDateFilterSet(search)).toBe(true);
  });

  it("trims input without splitting it into provider selections", () => {
    const query = getProtoFilterParams(new URLSearchParams({ agent: " claude,codex " }));

    expect(query.genericFilters?.[0].value?.stringValue).toEqual(["claude,codex"]);
    expect(query.genericFilters?.[0].negate).toBe(false);
    expect(isAnyNonDateFilterSet(new URLSearchParams({ agent: "  " }))).toBe(false);
  });

  it("removes the agent predicate when the facet is cleared", () => {
    const search = new URLSearchParams({ role: "CI", agent: "codex" });
    search.set(AGENT_PARAM_NAME, "");

    expect(getProtoFilterParams(search).genericFilters).toEqual([]);
    expect(getProtoFilterParams(search).role).toEqual(["CI"]);
    expect(isAnyNonDateFilterSet(new URLSearchParams())).toBe(false);
  });

  it("does not send Agent queries to a server without Agent filtering support", () => {
    capabilities.config.agentFilterEnabled = false;
    const search = new URLSearchParams({ agent: "codex", tag: "coverage", sq: "tag:release" });

    const query = getProtoFilterParams(search);
    expect(query.tags).toEqual(["coverage"]);
    expect(query.genericFilters?.length).toBe(1);
    expect(query.genericFilters?.[0].operand).toBe(stat_filter.FilterOperand.ARRAY_CONTAINS_OPERAND);
    expect(query.genericFilters?.[0].value?.stringValue).toEqual(["release"]);
    expect(isAnyNonDateFilterSet(new URLSearchParams({ agent: "codex" }))).toBe(false);
  });
});
