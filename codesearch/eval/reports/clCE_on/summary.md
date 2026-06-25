# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 56% | 88% | 0.69 | 80% | 96% | 0.88 | 0.33 | 80% |
| keyword | 20 | 20 | 65% | 90% | 0.79 | 70% | 90% | 0.78 | 0.57 | 85% |
| phrase | 15 | 15 | 73% | 93% | 0.82 | 40% | 80% | 0.57 | 0.84 | 80% |
| regex | 15 | 6 | 33% | 67% | 0.42 | 50% | 83% | 0.62 | 0.17 | 27% |
| filter | 10 | 5 | 80% | 100% | 0.85 | 60% | 100% | 0.80 | 0.68 | 80% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.31 | 60% |
| **total** | 100 | 71 | 62% | 89% | 0.73 | 65% | 90% | 0.76 | 0.46 | 70% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank 11, google rank 1, target src/fmt/print.go
- `GOMAXPROCS` (sym-gomaxprocs): local rank 7, google rank 1, target src/runtime/debug.go
- `template parse tree` (kw-template-parse-tree): local rank 6, google rank 1, target src/text/template/parse/parse.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 16, google rank 4, target src/fmt/print.go
