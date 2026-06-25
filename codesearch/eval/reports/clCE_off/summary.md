# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 12% | 72% | 0.40 | 80% | 96% | 0.88 | 0.39 | 68% |
| keyword | 20 | 20 | 40% | 85% | 0.62 | 70% | 90% | 0.78 | 0.55 | 80% |
| phrase | 15 | 15 | 60% | 93% | 0.72 | 40% | 80% | 0.57 | 0.83 | 87% |
| regex | 15 | 6 | 33% | 67% | 0.46 | 50% | 83% | 0.62 | 0.25 | 40% |
| filter | 10 | 5 | 20% | 80% | 0.44 | 60% | 100% | 0.80 | 0.70 | 90% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.36 | 73% |
| **total** | 100 | 71 | 32% | 80% | 0.54 | 65% | 90% | 0.76 | 0.49 | 72% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank miss, google rank 1, target src/fmt/print.go
- `mallocgc` (sym-mallocgc): local rank 6, google rank 1, target src/runtime/malloc.go
- `Goexit` (sym-goexit): local rank 8, google rank 1, target src/runtime/panic.go
- `QuoteMeta` (sym-quotemeta): local rank 8, google rank 1, target src/regexp/regexp.go
- `func Fields` (sym-fields): local rank miss, google rank 9, target src/strings/strings.go
- `utf8 decode rune` (kw-utf8-decode-rune): local rank 8, google rank 1, target src/unicode/utf8/utf8.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank miss, google rank 4, target src/fmt/print.go
- `case:yes Printf` (flt-case-printf): local rank 45, google rank 2, target src/fmt/print.go
