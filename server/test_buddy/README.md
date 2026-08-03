# TestBuddy

TestBuddy records target- and case-level results and projects them into fast,
queryable health state.

## Core design

- A target is addressed by group, normalized repository URL, and Bazel target
  label. A case adds its exact case name. These natural fields are the database
  keys; there are no synthetic test IDs, framework fields, or variants.
- Targets and cases are independent subjects. A case failure changes only that
  case. A target timeout or a failure that cannot be attributed to a case
  changes only the target. An attributable case failure still records a target
  pass because the target harness completed.
- Health uses a configurable per-repository window, defaulting to the 50 most
  recently processed samples. Once the failure threshold is met, a mixed
  pass/fail window is flaky while an all-failure window is failing. Target
  timeouts use a separate default
  threshold of five and produce a distinct timeout state. Three consecutive
  passes make a subject healthy once failure evidence has left the window.
- Reporting uses a bounded client stream. Each subject is updated in a small
  independent transaction, and the RPC returns after every streamed batch has
  committed. There is no queue or report-wide transaction.
- Each observation retains its execution time and a stable reporter-derived
  result ID. Analysis still follows processing order. Current state keeps the
  bounded evidence window plus 200 recent ID fingerprints, so an exact retry is
  a no-op while conflicting reuse is rejected.
- Heavy processing runs in the dedicated TestBuddy service. BuildBuddy apps
  authenticate and proxy RPCs, keeping report work off the app serving path.
- Reads never parse reports. Package-cone queries return failing, flaky, and
  timed-out subjects before healthy ones; exact reads return current health,
  aggregate statistics, recent evidence, and state-change history. Current state
  and each transition retain the analyzer revision, reason, and eligible sample
  count that produced them.
- A cone read is a range scan of the catalog's own
  `(group_id, repository, package_path)` index, bounded on the package separator
  so that `a/bc` stays outside the `a/b` cone. There is no routing table, prefix
  table, or hashed bucket: the package path a target already stores is the index.

`bb test-report` reads Bazel test output and reports it. JUnit is only an input
format and is not part of test identity.
