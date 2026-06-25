# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 80% | 96% | 0.88 | 80% | 96% | 0.88 | 76% | 92% | 0.84 | 0.40 | 88% |
| keyword | 20 | 20 | 65% | 85% | 0.73 | 70% | 90% | 0.78 | 55% | 90% | 0.70 | 0.58 | 95% |
| phrase | 15 | 15 | 73% | 93% | 0.82 | 40% | 80% | 0.57 | 80% | 93% | 0.85 | 0.91 | 93% |
| regex | 15 | 6 | 17% | 67% | 0.38 | 50% | 83% | 0.62 | 50% | 50% | 0.54 | 0.16 | 33% |
| filter | 10 | 5 | 60% | 100% | 0.71 | 60% | 100% | 0.80 | 60% | 100% | 0.77 | 0.80 | 90% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.33 | 53% |
| **total** | 100 | 71 | 68% | 90% | 0.77 | 65% | 90% | 0.76 | 68% | 89% | 0.77 | 0.51 | 77% |

## Queries where we miss the target but Google finds it

- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 20, google rank 4, target src/fmt/print.go
