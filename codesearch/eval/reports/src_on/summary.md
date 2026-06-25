# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 80% | 96% | 0.88 | 80% | 96% | 0.88 | 0.37 | 92% |
| keyword | 20 | 20 | 65% | 85% | 0.74 | 70% | 90% | 0.78 | 0.58 | 95% |
| phrase | 15 | 15 | 73% | 93% | 0.82 | 40% | 80% | 0.57 | 0.84 | 80% |
| regex | 15 | 6 | 17% | 50% | 0.37 | 50% | 83% | 0.62 | 0.17 | 27% |
| filter | 10 | 5 | 60% | 80% | 0.70 | 60% | 100% | 0.80 | 0.76 | 90% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.34 | 73% |
| **total** | 100 | 71 | 68% | 87% | 0.77 | 65% | 90% | 0.76 | 0.49 | 78% |

## Queries where we miss the target but Google finds it

- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 27, google rank 4, target src/fmt/print.go
