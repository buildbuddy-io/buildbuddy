# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 68% | 92% | 0.77 | 80% | 96% | 0.88 | 0.37 | 80% |
| keyword | 20 | 20 | 65% | 90% | 0.76 | 70% | 90% | 0.78 | 0.51 | 75% |
| phrase | 15 | 15 | 73% | 93% | 0.83 | 40% | 80% | 0.57 | 0.83 | 87% |
| regex | 15 | 6 | 17% | 50% | 0.34 | 50% | 83% | 0.62 | 0.15 | 27% |
| filter | 10 | 5 | 60% | 80% | 0.67 | 60% | 100% | 0.80 | 0.65 | 70% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.25 | 60% |
| **total** | 100 | 71 | 63% | 87% | 0.74 | 65% | 90% | 0.76 | 0.45 | 68% |

## Queries where we miss the target but Google finds it

- `func NewRequest` (sym-newrequest): local rank 8, google rank 1, target src/net/http/request.go
- `tls handshake client` (kw-tls-handshake-client): local rank 10, google rank 1, target src/crypto/tls/handshake_client.go
- `template parse tree` (kw-template-parse-tree): local rank 9, google rank 1, target src/text/template/parse/parse.go
- `EOF = errors\.New` (re-eof-error): local rank 10, google rank 1, target src/io/io.go
- `case:yes Printf` (flt-case-printf): local rank 12, google rank 2, target src/fmt/print.go
