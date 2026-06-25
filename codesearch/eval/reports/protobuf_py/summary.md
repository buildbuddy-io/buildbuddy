# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 10 | 10 | 50% | 90% | 0.69 | 90% | 100% | 0.95 | 0.49 | 80% |
| **total** | 10 | 10 | 50% | 90% | 0.69 | 90% | 100% | 0.95 | 0.49 | 80% |

## Queries where we miss the target but Google finds it

- `DescriptorPool` (py_descriptorpool): local rank 18, google rank 1, target python/google/protobuf/descriptor_pool.py
