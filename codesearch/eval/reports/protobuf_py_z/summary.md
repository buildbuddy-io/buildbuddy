# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 10 | 10 | 100% | 100% | 1.00 | 90% | 100% | 0.95 | 40% | 70% | 0.52 | 0.50 | 90% |
| **total** | 10 | 10 | 100% | 100% | 1.00 | 90% | 100% | 0.95 | 40% | 70% | 0.52 | 0.50 | 90% |

## Queries where we miss the target but Google finds it

(none)
