# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 80% | 96% | 0.88 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.35 | 88% |
| keyword | 20 | 20 | 60% | 90% | 0.72 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.58 | 90% |
| phrase | 15 | 15 | 20% | 67% | 0.40 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.26 | 47% |
| regex | 15 | 6 | 17% | 50% | 0.37 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.12 | 27% |
| filter | 10 | 5 | 60% | 100% | 0.71 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.49 | 80% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.33 | 67% |
| **total** | 100 | 71 | 55% | 85% | 0.68 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.36 | 69% |

## Queries where we miss the target but Google finds it

- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"context deadline exceeded"` (phr-deadline-exceeded): local rank 8, google rank 1, target src/context/context.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 18, google rank 4, target src/fmt/print.go
