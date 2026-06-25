# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 84% | 96% | 0.90 | 80% | 96% | 0.88 | 0.42 | 92% |
| keyword | 20 | 20 | 60% | 85% | 0.71 | 70% | 90% | 0.78 | 0.56 | 95% |
| phrase | 15 | 15 | 73% | 93% | 0.83 | 40% | 80% | 0.57 | 0.83 | 87% |
| regex | 15 | 6 | 17% | 67% | 0.35 | 50% | 83% | 0.62 | 0.14 | 33% |
| filter | 10 | 5 | 60% | 100% | 0.71 | 60% | 100% | 0.80 | 0.79 | 100% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.36 | 67% |
| **total** | 100 | 71 | 68% | 90% | 0.77 | 65% | 90% | 0.76 | 0.50 | 80% |

## Queries where we miss the target but Google finds it

- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 20, google rank 4, target src/fmt/print.go
