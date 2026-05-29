# castop

`castop` is an internal CLI for debugging large CAS downloads across recent
invocations. It scans invocations, fetches executor download details and Bazel
cache scorecard entries, and renders a live terminal table of the largest CAS
downloads found so far.

## Usage

Run the tool with `bb run`:

```bash
bb run -- //enterprise/tools/castop --api_key=KEY --group_id=GROUP_ID
```

To inspect a single invocation directly, pass `--invocation_id`. In this mode
`castop` skips `SearchInvocation` and only queries the requested invocation.

Use `--client=executor`, `--client=bazel`, or `--client=all` to choose which
download sources to include.

## How It Works

`castop` uses several worker pools:

- One `SearchInvocation` worker pages through recent invocations and prioritizes
  invocations with the largest total cache download size.
- `GetExecution` workers fetch executions for each invocation, sort them by
  input download size, and enqueue executions with nonzero CAS downloads.
- `GetExecutionDownloads` workers page through executor download records and
  report each result as client `executor`.
- `GetCacheScoreCard` workers page through invocation scorecard entries ordered
  by size and report each result as client `bazel`.
- The main goroutine aggregates all download records and renders the live UI.

Priority queues keep the work most likely to account for the largest remaining
CAS download volume near the front. Each invocation starts with its reported
total cache download size as "unaccounted for". As executor download pages and
scorecard pages are fetched, `castop` subtracts the bytes surfaced by those
pages. Queue priorities are refreshed when workers pop tasks, so work for the
invocations with the most unaccounted bytes jumps ahead even if older queued
tasks have stale priorities.

Within an invocation, executions with larger reported file download size are
queried first. Continuation pages for both executor downloads and scorecards
use a secondary priority estimate: the configured page size multiplied by the
smallest result from the current page. Since pages are sorted by size
descending, the next page cannot exceed that estimate. Executor download
continuations additionally cap that estimate by the remaining reported download
count and bytes, since `GetExecution` exposes those totals.

## UI

The live UI shows:

- Fetched object counts and ignored RPC error counts.
- RPC progress as `Done/Total`, where total is the number of tasks ever added to
  the corresponding queue.
- A timestamp progress strip. The newest fetched invocation is on the left and
  the oldest is on the right. Each cell is shaded by how much of that invocation
  has been fetched; when there are more invocations than terminal columns,
  adjacent invocations are averaged into each cell.
- A table of downloads grouped by name and client, sorted by total bytes.

On normal completion or Ctrl+C, `castop` prints the latest table snapshot back
to the console after leaving the live UI.

## Pagination Notes

`GetExecutionDownloadsRequest` has a real `page_size`, exposed as
`--get_execution_downloads_page_size`.

`GetCacheScoreCardRequest` does not currently expose `page_size`. The server
stores its page token as an internal `OffsetLimit`, so `castop` synthesizes
tokens with `--get_cache_scorecard_page_size` for broad scans. This is an
internal workaround; the API should grow a real page size field.

## API Improvements

- `SearchInvocation` should not require `group_id` when there are no other
  search atoms. If the request does not specify a group, user, host, repo, or
  commit filter, it should default to searching invocations from the
  authenticated user's group ID.
- `GetExecution` should expose `invocation_link_type` on returned executions.
  `remote_execution.StoredExecution` has this field already, but
  `execution_stats.Execution` does not appear to expose it today. `castop`
  could use this to avoid double counting executions across invocations instead
  of deduping by execution ID client side.
- `GetCacheScoreCardRequest` should expose a configurable `page_size`.
