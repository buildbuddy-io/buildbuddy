# Initial correctness corpus

Go: `go version go1.27.1 linux/amd64`

Supporting code/dependencies: `266066a57cb351ae38d5f5c71289ff94b2a49eb5`

Harness SHA-256: `11d6162757f5ff8e894dbf29afbed625e07932a702f28932be4767b9fec00b9d`

Detection requires the same generated scenario to fail on the first parent and pass on the fix. Other failures on the fix are retained in JSON, not counted as detections.

| Case | Fix | Baseline | Improved | Missing capabilities |
|---|---|---|---|---|
| range-panic | `0c58359feb` | missed | detected | operation coverage, input coverage |
| range-exclusive-end | `e7794fb342` | missed | detected | operation coverage, input coverage |
| range-before-first | `aea0bec7cc` | missed | detected | operation coverage, input coverage |
| range-diagnostic-text | `1223d351ef` | missed | detected | input coverage, invariant/oracle |
| retry-cancellation | `c0942dce5f` | missed | detected | environment or dependency behavior, input coverage |
| retry-terminal | `d5a42771d1` | missed | detected | input coverage, invariant/oracle |
| retry-error-observability | `3e3af7a67b` | missed | detected | input coverage, invariant/oracle |
| peer-failure-budget | `17a5e0b885` | missed | detected | sequence depth, configuration |
| peer-backfill-health | `34a38446c7` | missed | detected | operation coverage, sequence depth, invariant/oracle |
| sequence-consumption | `ae7e21e13d` | missed | detected | invariant/oracle, sequence depth |

## Improvements and reproducible evidence

### range-panic

Overlap enumeration was absent; include queries before, within, between and beyond stored intervals.

Generated witness: `TestRangeModel/count=1/query=0-2`

```text
=== RUN   TestRangeModel/count=1/query=0-2
    contract_harness_test.go:12: contract panic: runtime error: slice bounds out of range [-1:]
--- FAIL: TestRangeModel/count=1/query=0-2 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case range-panic`

### range-exclusive-end

Generate query endpoints equal to stored endpoints under the half-open interval model.

Generated witness: `TestRangeModel/count=2/query=0-4`

```text
=== RUN   TestRangeModel/count=2/query=0-4
    contract_harness_test.go:56: overlap got=[1 2] want=[1]
--- FAIL: TestRangeModel/count=2/query=0-4 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case range-exclusive-end`

### range-before-first

Include disjoint queries on both sides of the stored domain, including adjacency.

Generated witness: `TestRangeModel/count=1/query=0-1`

```text
=== RUN   TestRangeModel/count=1/query=0-1
    contract_harness_test.go:56: overlap got=[1] want=[]
--- FAIL: TestRangeModel/count=1/query=0-1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case range-before-first`

### range-diagnostic-text

Generate the entire byte alphabet and require diagnostics to remain valid UTF-8.

Generated witness: `TestRangeDiagnosticText/byte=128`

```text
=== RUN   TestRangeDiagnosticText/byte=128
    contract_harness_test.go:78: diagnostic is invalid UTF-8: "[\x80, \x80\x00)"
--- FAIL: TestRangeDiagnosticText/byte=128 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case range-diagnostic-text`

### retry-cancellation

Model a cancelled context with a blocked timer; require cancellation error propagation.

Generated witness: `TestRetryOutcomes/cancelled/budget=1`

```text
=== RUN   TestRetryOutcomes/cancelled/budget=1
    contract_harness_test.go:68: cancellation result err=<nil> attempts=0
--- FAIL: TestRetryOutcomes/cancelled/budget=1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case retry-cancellation`

### retry-terminal

Generate terminal and transient dependency outcomes and constrain terminal call count.

Generated witness: `TestRetryOutcomes/terminal/budget=1`

```text
=== RUN   TestRetryOutcomes/terminal/budget=1
    contract_harness_test.go:72: terminal dependency invoked 2 times
--- FAIL: TestRetryOutcomes/terminal/budget=1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case retry-terminal`

### retry-error-observability

Cross nullable and wrapped dependency outcomes with retry policies; require returned errors to be safely inspectable.

Generated witness: `TestRetryErrorObservability/input=0/terminal=true/budget=1`

```text
=== RUN   TestRetryErrorObservability/input=0/terminal=true/budget=1
    contract_harness_test.go:101: error inspection panicked: runtime error: invalid memory address or nil pointer dereference
--- FAIL: TestRetryErrorObservability/input=0/terminal=true/budget=1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case retry-error-observability`

### peer-failure-budget

Explore failure traces beyond the configured fallback budget for multiple peer-list sizes.

Generated witness: `TestPeerFailureBudget/fallbacks=4`

```text
=== RUN   TestPeerFailureBudget/fallbacks=4
    contract_harness_test.go:84: fallback attempts=4 budget=3 available=4
--- FAIL: TestPeerFailureBudget/fallbacks=4 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case peer-failure-budget`

### peer-backfill-health

Observe backfill after each success/failure transition and exclude failed endpoints.

Generated witness: `TestPeerRouting/preferred=1/failures=1`

```text
=== RUN   TestPeerRouting/preferred=1/failures=1
    contract_harness_test.go:41: failed peer used as backfill source: "p0"
--- FAIL: TestPeerRouting/preferred=1/failures=1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case peer-backfill-health`

### sequence-consumption

Read the residual stateful stream after taking a prefix; require conservation of the input.

Generated witness: `TestSequenceConservation/length=2/limit=1`

```text
=== RUN   TestSequenceConservation/length=2/limit=1
    contract_harness_test.go:40: stream lost or duplicated elements: prefix=[1] rest=[] input=[1 2]
--- FAIL: TestSequenceConservation/length=2/limit=1 (0.00s)
```

Replay both revisions: `python3 tools/correctness/run.py replay --report tools/correctness/results/evidence.json --case sequence-consumption`

