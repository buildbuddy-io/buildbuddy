# Correctness contracts

This is a small, executable improvement loop for BuildBuddy refactors. It starts
with 10 historical fixes in four Go utilities. It generates bounded families of
operations and inputs, checks general contracts, and measures which failures it
can distinguish from their fixed versions. It does not run the fixes' regression
tests or use the fixed implementation as an oracle.

The initial calibration improved from **0/10 to 10/10 detected cases**, with no
infrastructure errors. All **677 generated scenarios** pass on the initial current
tree. [The measured report](results/report.md) includes classifications and
executable replay commands. [Compact evidence](results/evidence.json) records
both revisions, compiler, source and harness hashes, counts, and one distinguishing
failure per case. These are measurements of a deliberately small, retrospectively
selected corpus, not a repository-wide detection rate or a blind evaluation.

## Run

From the repository root, with Python 3.10+, Git history containing the corpus
commits, and the Go toolchain required by the pinned `go.mod`:

```sh
# Gate a refactor using production sources from the working tree (including edits).
python3 tools/correctness/run.py check

# Reproduce the baseline, improve, and measure against every first parent + fix.
python3 tools/correctness/run.py calibrate

# Replay one small generated scenario against both revisions.
python3 tools/correctness/run.py replay \
  --report tools/correctness/results/evidence.json --case sequence-consumption

# Check a committed candidate or isolate a component.
python3 tools/correctness/run.py check --revision HEAD --package retry

# Verify that scoring does not mistake build errors, skips, or timeouts for bugs.
python3 -m unittest discover -s tools/correctness -p 'test_*.py'
```

There is no service to deploy, agent, database, Bazel setup, or new Python
dependency. Go may download its toolchain and existing module dependencies on a
cold machine. No shell commands are constructed from generated input. Runs use
temporary directories and leave the checkout untouched. Go's build/module cache
is reused; test results are never cached (`-count=1`).

`check` exits nonzero on a contract or infrastructure failure. `calibrate` exits
nonzero if its final profile has misses or infrastructure failures; baseline
misses are expected. `replay` succeeds only when the specified scenario fails on
the parent and passes on the fix. Unknown or skipped test selections cannot pass.
Replay refuses mismatching harness hashes or compiler/platform versions.

Raw Go JSON logs, full measurements, compact replay evidence, and a readable report
are written under `.runs/` (ignored). Use `--out PATH` to retain a run elsewhere.
The committed evidence intentionally retains only one witness per fix; a fresh
calibration keeps every test outcome and every distinguishing witness.

## What is tested

| Component | Baseline capability | General improvement |
|---|---|---|
| Range map | Add nonoverlapping ranges; compare point lookup to interval arithmetic | Enumerate overlap queries across a finite endpoint domain; compare ordered results to a linear half-open interval model; require diagnostics for every byte value to be valid UTF-8 |
| Retry | Successful and transient dependency outcomes | Generate terminal and cancelled outcomes across retry budgets; control timer readiness; check call counts, cancellation propagation, and safe inspection of errors across nullable/wrapped dependency outcomes |
| Peer set | One preferred-peer operation | Enumerate preferred failure masks, query backfill after each transition, reject unhealthy endpoints, and exhaust fallback failure budgets across peer-list sizes |
| Sequence | Returned prefix equals a slice prefix | Consume the residual stateful stream and require prefix + remainder to equal the original input |

Both profiles remain executable. `HARNESS_PROFILE` only changes general coverage
or observations; drivers never inspect a fix ID. The source adapters handle API
shapes (generic range maps, `Take` versus `Truncate`, and availability of the
fallback-budget constant) at test call sites. Production code is never rewritten.
Unsupported historical budget APIs explicitly skip that property; a skip is never
a positive control.

Generation is exhaustive within small finite domains, with no random seed. Retry
uses a synchronous clock model with ready or blocked timers, so cancelled-context
tests cannot race a zero-duration timer. Stateful sequences are deterministic
in-memory iterators. There are no wall-clock assertions or probabilistic memory
measurements. The naturally smallest distinguishing generated scenario is retained
for replay; no separate shrinking infrastructure is needed for this corpus.

## Historical experiment and limitations

`corpus.json` freezes exactly 10 fixes and their full first-parent hashes. The
runner verifies that relationship before calibration. Each run copies all the
chosen utility's production `.go` files verbatim from that revision, excluding
historical tests, and injects the same contract driver used for current checks.

To keep the experiment small, **supporting in-repository packages and Go modules
are pinned to `support_revision`**, initially
`266066a57cb351ae38d5f5c71289ff94b2a49eb5`. Only the selected component moves to its
historical revision. Supporting code is real production code, recursively copied
from the pin; there are no logger/status stubs. The pinned `go.mod` and `go.sum`
are used read-only. This is component snapshot validation in a controlled modern
dependency environment, **not execution of each entire historical repository**.
It cannot establish behavior with the original historical compiler/dependencies.
Current checks likewise cover the selected components, not edits to supporting
packages; deliberately update the support pin when expanding that scope.

A case is detected only if a named generated leaf scenario fails on the parent
and passes on the fix. A failed compile, timeout, missing test, or skipped test
does not count. Some early fixed revisions still contain later corpus bugs, so
their whole suite can remain red. Full reports retain those failures; detection
requires a distinguishing scenario, not just a failing parent process. Even this
paired evidence establishes a behavior change, not an automatic causal proof
about every line of a multi-change commit; corpus review remains necessary.

The `retry-error-observability` fix also changed the documented error identity
contract. The harness deliberately does not count that API change as a bug. It
detects a malformed returned error whose `Error()` method panics when a nil
dependency outcome is marked terminal; the fix added safe handling of that case.
This falls out of the nullable-outcome / retry-policy cross product and the
general requirement that returned errors can be inspected without panicking.

Concurrency, distributed services, storage, authentication, real network faults,
and dependency-version variation remain outside this initial experiment. No
concurrency detection claim is made. The corpus favors small locally executable
utilities; do not silently substitute easy cases for misses as scope grows.

## Keep improving

1. Run `check` before accepting a simplification or refactor. The command is also
   suitable for a CI step with the required Git objects available. It is not
   added to the repository's broad Bazel suite, which does not provide history.
2. Re-run `calibrate` after changing the harness. Preserve the baseline profile
   and retain both the detection counts and current-tree results.
3. For each miss, classify the gap in the corpus: operation coverage, input
   coverage, invariant/oracle, sequence depth, concurrency, configuration,
   environment/dependency behavior, or an explicitly explained new category.
4. Improve a generator, operation model, dependency model, or reusable contract.
   Do not branch on commit IDs or add the historical input as an assertion.
   Re-run the *entire same corpus*, the current tree, and each saved witness.
5. Review and retain `evidence.json` and `report.md` together. Recalibration changes
   the evidence fingerprint when the runner, drivers, or corpus change. For
   retained reports, generate their Markdown beside their evidence file so replay
   paths remain correct.
6. Only then expand beyond the initial 10. Keep old cases and openly report
   misses and infrastructure blocks. Agent-generated hypotheses may suggest
   capabilities to add, but are findings only after executable reproduction.

The initial loop used one improvement round: the preserved baseline missed all
10, each miss was classified, and the four general driver improvements above
detected all 10. Further rounds should add profiles rather than retroactively
changing what the baseline measured.
