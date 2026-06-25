# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 10 | 10 | 90% | 100% | 0.95 | 100% | 100% | 1.00 | 10% | 60% | 0.39 | 0.75 | 100% |
| keyword | 5 | 5 | 80% | 100% | 0.85 | 80% | 100% | 0.84 | 80% | 100% | 0.85 | 0.93 | 100% |
| phrase | 4 | 4 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 1.00 | 100% |
| regex | 3 | 3 | 67% | 100% | 0.83 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 1.00 | 100% |
| **total** | 22 | 22 | 86% | 100% | 0.92 | 95% | 100% | 0.96 | 55% | 82% | 0.69 | 0.87 | 100% |

## Queries where we miss the target but Google finds it

(none)
