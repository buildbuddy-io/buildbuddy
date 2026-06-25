# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 12% | 76% | 0.38 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.36 | 68% |
| keyword | 20 | 20 | 40% | 85% | 0.62 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.56 | 80% |
| phrase | 15 | 15 | 33% | 53% | 0.43 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.27 | 47% |
| regex | 15 | 6 | 33% | 33% | 0.36 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.17 | 33% |
| filter | 10 | 5 | 20% | 60% | 0.39 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.46 | 80% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.37 | 73% |
| **total** | 100 | 71 | 27% | 69% | 0.46 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.37 | 64% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank miss, google rank 1, target src/fmt/print.go
- `mallocgc` (sym-mallocgc): local rank 6, google rank 1, target src/runtime/malloc.go
- `Goexit` (sym-goexit): local rank 7, google rank 1, target src/runtime/panic.go
- `QuoteMeta` (sym-quotemeta): local rank 40, google rank 1, target src/regexp/regexp.go
- `func ReadAll` (sym-readall): local rank 13, google rank 2, target src/io/io.go
- `func Fields` (sym-fields): local rank miss, google rank 9, target src/strings/strings.go
- `utf8 decode rune` (kw-utf8-decode-rune): local rank 8, google rank 1, target src/unicode/utf8/utf8.go
- `"use of closed network connection"` (phr-closed-network): local rank 13, google rank 4, target src/internal/poll/fd.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"unexpected end of JSON input"` (phr-json-eof): local rank 12, google rank 3, target src/encoding/json/scanner.go
- `"bad certificate"` (phr-bad-certificate): local rank 6, google rank 1, target src/crypto/tls/alert.go
- `"runtime: out of memory"` (phr-out-of-memory): local rank 11, google rank 3, target src/runtime/malloc.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank miss, google rank 4, target src/fmt/print.go
- `func \(b \*Buffer\)` (re-buffer-methods): local rank 13, google rank 2, target src/bytes/buffer.go
- `func \(c \*Conn\) Read` (re-conn-read): local rank 9, google rank 1, target src/crypto/tls/conn.go
- `lang:go file:print.go Fprintf` (flt-print-fprintf): local rank 8, google rank 1, target src/fmt/print.go
- `case:yes Printf` (flt-case-printf): local rank miss, google rank 2, target src/fmt/print.go
