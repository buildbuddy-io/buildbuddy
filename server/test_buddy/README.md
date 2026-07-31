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
  recently processed samples. One failure makes a case or target flaky; target
  timeouts use a separate default threshold of five and produce a distinct
  timeout state. Three consecutive passes make a subject healthy once failure
  evidence has left the window.
- Reporting is synchronous. Each subject is updated in a small independent
  transaction, and the RPC returns after every update has committed. There is
  no queue or report-wide transaction.
- Heavy processing runs in the dedicated TestBuddy service. BuildBuddy apps
  authenticate and proxy RPCs, keeping report work off the app serving path.
- Reads never parse reports. Package-cone queries return flaky subjects first;
  exact reads return current health, aggregate statistics, recent evidence, and
  state-change history.
- Each target hashes to one of 4,096 stable logical buckets. Admission maps the
  target's package and every ancestor package to that bucket; cone reads use the
  mapping to find candidate buckets, then apply exact package bounds. A future
  router can move logical buckets between databases without changing test keys.

`bb test-report` reads Bazel test output and reports it. JUnit is only an input
format and is not part of test identity.
