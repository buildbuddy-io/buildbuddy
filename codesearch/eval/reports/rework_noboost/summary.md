# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 52% | 92% | 0.70 | 80% | 96% | 0.88 | 0.45 | 88% |
| keyword | 20 | 20 | 55% | 80% | 0.65 | 70% | 90% | 0.78 | 0.60 | 90% |
| phrase | 15 | 15 | 60% | 93% | 0.70 | 40% | 80% | 0.57 | 0.91 | 87% |
| regex | 15 | 6 | 17% | 33% | 0.24 | 50% | 83% | 0.62 | 0.23 | 33% |
| filter | 10 | 5 | 60% | 80% | 0.65 | 60% | 100% | 0.80 | 0.80 | 90% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.32 | 73% |
| **total** | 100 | 71 | 52% | 83% | 0.65 | 65% | 90% | 0.76 | 0.53 | 78% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank 6, google rank 1, target src/fmt/print.go
- `utf8 decode rune` (kw-utf8-decode-rune): local rank 10, google rank 1, target src/unicode/utf8/utf8.go
- `channel send receive` (kw-channel-send-receive): local rank 11, google rank 3, target src/runtime/chan.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank miss, google rank 4, target src/fmt/print.go
- `func \(b \*Buffer\)` (re-buffer-methods): local rank 15, google rank 2, target src/bytes/buffer.go
- `func \(c \*Conn\) Read` (re-conn-read): local rank 8, google rank 1, target src/crypto/tls/conn.go
- `case:yes Printf` (flt-case-printf): local rank 16, google rank 2, target src/fmt/print.go
