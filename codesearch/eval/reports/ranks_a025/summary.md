# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 8% | 40% | 0.24 | 80% | 96% | 0.88 | 0.35 | 40% |
| keyword | 20 | 20 | 25% | 75% | 0.43 | 70% | 90% | 0.78 | 0.43 | 70% |
| phrase | 15 | 15 | 27% | 80% | 0.47 | 40% | 80% | 0.57 | 0.34 | 67% |
| regex | 15 | 6 | 0% | 17% | 0.07 | 50% | 83% | 0.62 | 0.18 | 33% |
| filter | 10 | 5 | 20% | 20% | 0.25 | 60% | 100% | 0.80 | 0.34 | 30% |
| ranking | 15 | 0 | - | - | - | - | - | - | 0.16 | 27% |
| **total** | 100 | 71 | 17% | 55% | 0.33 | 65% | 90% | 0.76 | 0.31 | 46% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank miss, google rank 1, target src/fmt/print.go
- `WaitGroup` (sym-waitgroup): local rank 6, google rank 1, target src/sync/waitgroup.go
- `func ListenAndServe` (sym-listenandserve): local rank 8, google rank 1, target src/net/http/server.go
- `NewScanner` (sym-newscanner): local rank 10, google rank 1, target src/bufio/scan.go
- `gopanic` (sym-gopanic): local rank 8, google rank 1, target src/runtime/panic.go
- `mallocgc` (sym-mallocgc): local rank 9, google rank 1, target src/runtime/malloc.go
- `Goexit` (sym-goexit): local rank 31, google rank 1, target src/runtime/panic.go
- `Hijacker` (sym-hijacker): local rank 6, google rank 1, target src/net/http/server.go
- `LittleEndian` (sym-littleendian): local rank 25, google rank 3, target src/encoding/binary/binary.go
- `GOMAXPROCS` (sym-gomaxprocs): local rank 7, google rank 1, target src/runtime/debug.go
- `stopTheWorld` (sym-stoptheworld): local rank 9, google rank 1, target src/runtime/proc.go
- `QuoteMeta` (sym-quotemeta): local rank 45, google rank 1, target src/regexp/regexp.go
- `func ReadAll` (sym-readall): local rank 11, google rank 2, target src/io/io.go
- `func Fields` (sym-fields): local rank miss, google rank 9, target src/strings/strings.go
- `tls handshake client` (kw-tls-handshake-client): local rank 15, google rank 1, target src/crypto/tls/handshake_client.go
- `template parse tree` (kw-template-parse-tree): local rank 8, google rank 1, target src/text/template/parse/parse.go
- `utf8 decode rune` (kw-utf8-decode-rune): local rank 8, google rank 1, target src/unicode/utf8/utf8.go
- `channel send receive` (kw-channel-send-receive): local rank 11, google rank 3, target src/runtime/chan.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 7, google rank 1, target src/runtime/map_noswiss.go
- `"declared and not used"` (phr-declared-not-used): local rank 41, google rank 14, target src/cmd/compile/internal/types2/stmt.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank miss, google rank 4, target src/fmt/print.go
- `ListenAndServe(TLS)?` (re-listenandserve-tls): local rank 8, google rank 1, target src/net/http/server.go
- `EOF = errors\.New` (re-eof-error): local rank miss, google rank 1, target src/io/io.go
- `func \(c \*Conn\) Read` (re-conn-read): local rank 20, google rank 1, target src/crypto/tls/conn.go
- `file:waitgroup.go Add` (flt-waitgroup-add): local rank 7, google rank 1, target src/sync/waitgroup.go
- `lang:go file:print.go Fprintf` (flt-print-fprintf): local rank 11, google rank 1, target src/fmt/print.go
- `case:yes Printf` (flt-case-printf): local rank miss, google rank 2, target src/fmt/print.go
- `file:json decode` (flt-json-decode): local rank 26, google rank 2, target src/encoding/json/decode.go
