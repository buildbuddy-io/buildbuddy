# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 10 | 10 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 0.75 | 100% |
| keyword | 5 | 5 | 80% | 100% | 0.85 | 80% | 100% | 0.84 | 0.93 | 100% |
| phrase | 4 | 4 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 1.00 | 100% |
| regex | 3 | 3 | 67% | 100% | 0.83 | 100% | 100% | 1.00 | 1.00 | 100% |
| **total** | 22 | 22 | 91% | 100% | 0.94 | 95% | 100% | 0.96 | 0.87 | 100% |

## Queries where we miss the target but Google finds it

(none)
