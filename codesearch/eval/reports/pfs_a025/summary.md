# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 48% | 84% | 0.64 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.29 | 76% |
| keyword | 20 | 20 | 65% | 95% | 0.78 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.57 | 90% |
| phrase | 15 | 15 | 20% | 67% | 0.40 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.26 | 47% |
| regex | 15 | 6 | 33% | 67% | 0.44 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.13 | 20% |
| filter | 10 | 5 | 80% | 100% | 0.84 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.43 | 70% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.32 | 73% |
| **total** | 100 | 71 | 48% | 83% | 0.62 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.34 | 65% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank 11, google rank 1, target src/fmt/print.go
- `GOMAXPROCS` (sym-gomaxprocs): local rank 7, google rank 1, target src/runtime/debug.go
- `QuoteMeta` (sym-quotemeta): local rank 42, google rank 1, target src/regexp/regexp.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"context deadline exceeded"` (phr-deadline-exceeded): local rank 8, google rank 1, target src/context/context.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 16, google rank 4, target src/fmt/print.go
