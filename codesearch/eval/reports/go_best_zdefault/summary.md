# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 80% | 96% | 0.88 | 80% | 96% | 0.88 | 76% | 92% | 0.84 | 0.35 | 92% |
| keyword | 20 | 20 | 65% | 85% | 0.74 | 70% | 90% | 0.78 | 55% | 90% | 0.70 | 0.59 | 95% |
| phrase | 15 | 15 | 20% | 67% | 0.40 | 40% | 80% | 0.57 | 80% | 93% | 0.85 | 0.26 | 47% |
| regex | 15 | 6 | 17% | 50% | 0.37 | 50% | 83% | 0.62 | 50% | 50% | 0.54 | 0.11 | 20% |
| filter | 10 | 5 | 60% | 80% | 0.70 | 60% | 100% | 0.80 | 60% | 100% | 0.77 | 0.48 | 80% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.33 | 73% |
| **total** | 100 | 71 | 56% | 82% | 0.68 | 65% | 90% | 0.76 | 68% | 89% | 0.77 | 0.36 | 71% |

## Queries where we miss the target but Google finds it

- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"context deadline exceeded"` (phr-deadline-exceeded): local rank 8, google rank 1, target src/context/context.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 27, google rank 4, target src/fmt/print.go
