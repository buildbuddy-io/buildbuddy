# TestBuddy

TestBuddy records target- and case-level observations and projects them into
fast, queryable health state.

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
  timeouts use a separate default threshold of five and produce a distinct
  timeout state. A non-empty all-pass window is healthy immediately; three
  consecutive passes recover mixed evidence that remains below its configured
  failure threshold.
- Reporting uses a bounded client stream. Each subject is updated in a small
  independent transaction, and the RPC returns after every streamed batch has
  committed. There is no queue or report-wide transaction.
- Monitor and postsubmit observations contribute to health; presubmit reports
  discover targets and cases but do not change state, statistics, transitions,
  or recent evidence. Eligible observations retain the tested commit and
  dirty-checkout bit, execution time, source, and a stable reporter-derived ID.
  Analysis follows processing order. Current state keeps the bounded evidence
  window plus 200 recent ID content digests, so an exact retry is a no-op while
  conflicting reuse is rejected.
- Heavy processing runs in the dedicated TestBuddy service. BuildBuddy apps
  authenticate and proxy RPCs, keeping report work off the app serving path.
- CI test invocations automatically report target observations from BES.
  `main` and `master` are postsubmit; every other branch is presubmit. Running
  `bb test-report --invocation_id=...` reports cases without sending targets
  again when its invocation API and report RPC use that same app. Cross-backend
  imports, including the local prototype importing a hosted invocation, report
  both targets and cases.
- Reads never parse reports. Package-cone queries return failing, flaky, and
  timed-out subjects before healthy ones. Target rows expose target health and a
  separate rollup of their cases; case failures affect ordering but never change
  target state. Exact reads return current health, aggregate statistics, recent
  evidence, and state-change history. Failure observations retain bounded raw
  messages that the UI opens as plain text. Current state and each transition
  retain the analyzer revision, reason, and eligible sample count that produced
  them.
- Execution disposition is separate from observed health. Automatic follows
  health, enabled forces a subject to run, and disabled forces it to be skipped.
  The streamed cone query for tests to skip returns target and case identities
  with their health summaries and dispositions, not names alone.
- Deletion is a reversible catalog tombstone, separate from health and
  execution disposition. Deleted subjects stay available to exact
  administrative reads but disappear from normal and skip queries; reporting
  the same address restores it.
- A cone read is a range scan of the catalog's own
  `(group_id, repository, package_path)` index, bounded on the package separator
  so that `a/bc` stays outside the `a/b` cone. There is no routing table, prefix
  table, or hashed bucket: the package path a target already stores is the index.

`bb test-report` reads Bazel test output and reports it as monitor evidence by
default. For a CI invocation it derives presubmit or postsubmit from the branch;
`--source` overrides that default.
After `bb remote test`, pass its Bazel invocation ID as
`bb test-report --invocation_id=<id>` to fetch and report remote `test.xml`
artifacts directly, without a local `bazel-testlogs` tree.
JUnit is only an input format and is not part of test identity.
