# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 32% | 64% | 0.47 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.34 | 56% |
| keyword | 20 | 20 | 45% | 90% | 0.62 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.43 | 75% |
| phrase | 15 | 15 | 60% | 80% | 0.66 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.33 | 60% |
| regex | 15 | 6 | 0% | 17% | 0.16 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.17 | 47% |
| filter | 10 | 5 | 20% | 40% | 0.29 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.35 | 50% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.12 | 33% |
| **total** | 100 | 71 | 38% | 69% | 0.51 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.30 | 55% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank miss, google rank 1, target src/fmt/print.go
- `func ListenAndServe` (sym-listenandserve): local rank 8, google rank 1, target src/net/http/server.go
- `NewScanner` (sym-newscanner): local rank 6, google rank 1, target src/bufio/scan.go
- `mallocgc` (sym-mallocgc): local rank 8, google rank 1, target src/runtime/malloc.go
- `Goexit` (sym-goexit): local rank 20, google rank 1, target src/runtime/panic.go
- `LittleEndian` (sym-littleendian): local rank 19, google rank 3, target src/encoding/binary/binary.go
- `stopTheWorld` (sym-stoptheworld): local rank 8, google rank 1, target src/runtime/proc.go
- `QuoteMeta` (sym-quotemeta): local rank 30, google rank 1, target src/regexp/regexp.go
- `func Fields` (sym-fields): local rank miss, google rank 9, target src/strings/strings.go
- `tls handshake client` (kw-tls-handshake-client): local rank 8, google rank 1, target src/crypto/tls/handshake_client.go
- `template parse tree` (kw-template-parse-tree): local rank 6, google rank 1, target src/text/template/parse/parse.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"declared and not used"` (phr-declared-not-used): local rank 33, google rank 14, target src/cmd/compile/internal/types2/stmt.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 19, google rank 4, target src/fmt/print.go
- `ListenAndServe(TLS)?` (re-listenandserve-tls): local rank 7, google rank 1, target src/net/http/server.go
- `EOF = errors\.New` (re-eof-error): local rank 11, google rank 1, target src/io/io.go
- `func \(c \*Conn\) Read` (re-conn-read): local rank 6, google rank 1, target src/crypto/tls/conn.go
- `lang:go file:print.go Fprintf` (flt-print-fprintf): local rank 9, google rank 1, target src/fmt/print.go
- `case:yes Printf` (flt-case-printf): local rank miss, google rank 2, target src/fmt/print.go
- `file:json decode` (flt-json-decode): local rank 15, google rank 2, target src/encoding/json/decode.go
