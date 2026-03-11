# BuildBuddy MCP Work Summary and Roadmap

Last updated: 2026-02-22

## Goal
Build an MCP server in the BuildBuddy CLI so agents can query BuildBuddy invocation data and run higher-level build analysis workflows directly through tools.

## What We Have Built So Far

### 1) MCP server in CLI (`bb mcp`)
- Added a new CLI command: `bb mcp`.
- Server runs over stdio and is now built using the official `github.com/modelcontextprotocol/go-sdk`.
- Tool registration is centralized in a map literal.
- The command help text was cleaned up for users:
  - no "scaffold" wording
  - includes Claude/Codex/Gemini setup commands with `mcp add`
- Implemented in:
  - `cli/mcp/mcp.go`
  - command registration in `cli/cli_command/register/register.go`

### 2) Tool surface area added
Current MCP tools:
- `get_invocation`
  - Input: invocation ID or invocation URL
  - Output: invocation metadata (repo, commit, status, duration, command, etc.)
- `search_invocations`
  - Input: filters (`user`, `all_users`, `repo_url`, `role`, `command`, time bounds, paging, etc.)
  - Behavior: defaults to current Linux username when `user` is omitted and `all_users=false`
- `get_target`
  - Input: invocation + optional target filters/status/page token
  - Output: grouped targets/artifacts from `GetTarget`
- `analyze_profile`
  - Input: one or many invocation IDs/URLs
  - Runs profile analysis remotely in parallel and returns per-invocation + aggregated results

Tool implementations are in:
- `cli/mcp/mcptools/mcptools.go`

### 3) Auth model simplification
- Removed dedicated MCP `api_key` argument.
- Auth is resolved per tool call via `login.GetAPIKey()`.
- Supported key sources are now the standard CLI flow:
  - `BUILDBUDDY_API_KEY`
  - local repo `.git/config` (through existing login/key lookup behavior)
- This means `bb mcp` can start quickly and defer auth failures to actual tool calls.

### 4) Search behavior fixes
We adjusted query behavior after timeout and ergonomics issues:
- `search_invocations` no longer auto-applies `lookback_days` or `count` unless provided.
- Defaults are less opinionated for broad queries.
- Keeps user filtering default (current Linux username) unless explicitly overridden by `all_users=true` or `user=...`.

### 5) Timing-profile analysis architecture (major work)
`analyze_profile` was redesigned for scalability:

#### High-level flow
1. Resolve invocation -> timing profile file from BES
2. Resolve profile URI -> `ProfileResourceName`
3. Ensure profile digest exists in CAS
   - If missing, fetch via Remote Asset API from BuildBuddy `/file/download` URL
4. Run remote execution action(s) to analyze profile(s)
5. Aggregate results across invocations

#### Performance/concurrency settings
- Invocation/BES/profile-availability prep runs in parallel (fixed moderate limit)
- RE analysis runs with high concurrency:
  - default `parallelism = 100`
  - max `parallelism = 1000`
- RE action timeout reduced to `45s`
- gRPC connection pooling is used via `grpc_client.DialSimple`

#### Caching
- Added in-memory cache: `invocation_id -> timing_profile_resource_name` to avoid repeated BES/profile resolution.

### 6) Analyzer binary strategy (to avoid local libc/runtime issues)
Instead of local analysis dependencies:
- Added standalone analyzer binary entrypoint:
  - `cli/cmd/bb_analyze_profile/main.go` (binary name `bb-analyze-profile`)
- Added reusable local CLI command:
  - `bb analyze-profile`
  - `cli/analyze_profile/analyze_profile.go`
- Added profile summary package:
  - `cli/timing_profile/timing_profile.go`

For MCP remote analysis:
- Analyzer binary is fetched into cache via Remote Asset API (from GCS URL + checksum)
- Timing profile is referenced by digest (not downloaded locally)
- RE input root contains:
  - analyzer binary digest
  - timing profile digest

### 7) Trace parsing and summary expansion
Extended Bazel trace summary model in:
- `server/util/trace_events/trace_events.go`

Added fields used by MCP insights/diagnostics:
- `signal_durations_usec`
  - critical path
  - gc / major gc
  - action processing
  - remote action execution
  - remote action cache check
  - local action execution
  - queueing
- `has_merged_events`

Also updated package comments to be Bazel-focused and removed external analyzer-doc embedding and the specific example-invocation comment as requested.

### 8) New MCP output shape for analysis
`analyze_profile` now returns:
- `invocations[]` (each with raw summary + tool metadata)
- `aggregate` (existing top spans/categories/event totals)
- `insights` (new, structured)
  - critical path
  - garbage collection
  - jobs
  - bottlenecks
  - local actions with RE
- `diagnostics` (new, structured)
  - incomplete profile
  - merged events

Heuristic thresholds currently used:
- high GC: >= 5%
- jobs opportunity: critical path share of action-processing < 75%
- queueing bottleneck: queueing share >= 20%

### 9) Code organization work
- Moved MCP tool definitions/impl into `cli/mcp/mcptools` while keeping server loop in `cli/mcp/mcp.go`.
- Removed extra schema package churn by consolidating JSON schema/types where they are used.
- Reordered files for readability:
  - in `trace_events.go`, core event types/writer first, aggregation logic later
  - in `mcptools.go`, generic MCP/shared utilities first, analyze-timing section grouped later

## Current Behavior Snapshot

### Running the server
```bash
bb mcp
```

### Quick setup commands (from help)
```bash
# claude
claude mcp add --scope project buildbuddy -- bb mcp

# codex
codex mcp add buildbuddy -- bb mcp

# gemini
gemini mcp add --scope project buildbuddy bb mcp
```

### Key implementation files
- `cli/mcp/mcp.go`
- `cli/mcp/mcptools/mcptools.go`
- `cli/analyze_profile/analyze_profile.go`
- `cli/cmd/bb_analyze_profile/main.go`
- `cli/timing_profile/timing_profile.go`
- `server/util/trace_events/trace_events.go`

## Known Gaps / Caveats

1. Insight heuristics are currently signal/category-name based.
- They are useful, but not yet equivalent to full UI-level scoring logic.

2. Critical path output is summarized, not full-path extraction.
- No optional "full critical path dump" mode yet.

3. Jobs and bottleneck recommendations are aggregate heuristics.
- We still need stronger use of action-count time series and richer critical-path structure.

4. Diagnostics are intentionally separate from optimization insights.
- Incomplete profile and merged-events are presented as guidance/quality signals.

## Where We’re Headed (Proposed Next Steps)

### A) Strengthen analysis quality (highest priority)
1. Add a richer trace analysis model for:
- action count timeseries (parallelism over time)
- resource counters (CPU/memory/network/load)
- more precise queueing decomposition
2. Add optional full critical path extraction mode for deep dives.
3. Align recommendation logic with UI behavior (especially timing-card style scoring).

### B) Improve aggregate insights across many invocations
1. Cross-invocation ranking of recurring bottleneck signatures.
2. Grouping by branch/role/command to compare cohorts.
3. Better percentiles and outlier detection (P50/P90/P99 duration breakdown by signal).

### C) Improve robustness and developer confidence
1. Add unit tests around:
- trace signal extraction
- diagnostic classification
- aggregate insight math
2. Add integration/smoke tests for MCP tool calls (including remote-asset fetch paths).
3. Add golden JSON snapshots for `analyze_profile` outputs.

### D) UX improvements for agent users
1. Return clearer remediation text alongside numeric insights.
2. Add explicit links between diagnostics and suggested rerun flags.
3. Consider lightweight presets ("quick", "deep") for analysis output verbosity.

## Short Changelog (Narrative)
- Scaffolded MCP server and wired CLI command.
- Expanded from a single metadata tool to a practical toolset (`get_invocation`, `search_invocations`, `get_target`, `analyze_profile`).
- Switched to official MCP SDK transport/server implementation.
- Simplified auth by removing explicit MCP key args and relying on existing CLI key discovery.
- Reworked search defaults to avoid expensive accidental queries.
- Re-architected profile analysis for parallel remote execution.
- Solved missing-CAS profile issues with Remote Asset fetch + digest validation.
- Added standalone analyzer binary path and remote binary fetch strategy.
- Added structured insights/diagnostics and aggregate rollups.
- Cleaned docs/comments and reorganized code for maintainability.
