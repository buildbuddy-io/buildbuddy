# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 12% | 68% | 0.37 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.31 | 68% |
| keyword | 20 | 20 | 15% | 75% | 0.42 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.38 | 60% |
| phrase | 15 | 15 | 33% | 53% | 0.43 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.27 | 47% |
| regex | 15 | 6 | 17% | 33% | 0.22 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.15 | 20% |
| filter | 10 | 5 | 20% | 20% | 0.27 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.39 | 60% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.15 | 27% |
| **total** | 100 | 71 | 18% | 61% | 0.38 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.28 | 49% |

## Queries where we miss the target but Google finds it

- `func Fprintf` (sym-fprintf): local rank miss, google rank 1, target src/fmt/print.go
- `mallocgc` (sym-mallocgc): local rank 6, google rank 1, target src/runtime/malloc.go
- `Goexit` (sym-goexit): local rank 6, google rank 1, target src/runtime/panic.go
- `QuoteMeta` (sym-quotemeta): local rank 40, google rank 1, target src/regexp/regexp.go
- `func Pipe` (sym-pipe): local rank miss, google rank 2, target src/io/pipe.go
- `func ReadAll` (sym-readall): local rank 13, google rank 2, target src/io/io.go
- `hmac New` (sym-hmac): local rank 16, google rank 2, target src/crypto/hmac/hmac.go
- `func Fields` (sym-fields): local rank miss, google rank 9, target src/strings/strings.go
- `tls handshake client` (kw-tls-handshake-client): local rank 9, google rank 1, target src/crypto/tls/handshake_client.go
- `gzip reader` (kw-gzip-reader): local rank 11, google rank 2, target src/compress/gzip/gunzip.go
- `template parse tree` (kw-template-parse-tree): local rank 7, google rank 1, target src/text/template/parse/parse.go
- `sha256 block` (kw-sha256-block): local rank 26, google rank 8, target src/crypto/sha256/sha256.go
- `sendfile` (kw-sendfile): local rank 7, google rank 1, target src/net/sendfile_unix_alt.go
- `"use of closed network connection"` (phr-closed-network): local rank 13, google rank 4, target src/internal/poll/fd.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"unexpected end of JSON input"` (phr-json-eof): local rank 12, google rank 3, target src/encoding/json/scanner.go
- `"bad certificate"` (phr-bad-certificate): local rank 6, google rank 1, target src/crypto/tls/alert.go
- `"runtime: out of memory"` (phr-out-of-memory): local rank 11, google rank 3, target src/runtime/malloc.go
- `func (Fpr|Spr)intf` (re-fprintf-sprintf): local rank 45, google rank 4, target src/fmt/print.go
- `EOF = errors\.New` (re-eof-error): local rank 49, google rank 1, target src/io/io.go
- `func \(c \*Conn\) Read` (re-conn-read): local rank 22, google rank 1, target src/crypto/tls/conn.go
- `file:waitgroup.go Add` (flt-waitgroup-add): local rank 7, google rank 1, target src/sync/waitgroup.go
- `lang:go file:print.go Fprintf` (flt-print-fprintf): local rank 9, google rank 1, target src/fmt/print.go
- `case:yes Printf` (flt-case-printf): local rank miss, google rank 2, target src/fmt/print.go
- `file:json decode` (flt-json-decode): local rank 14, google rank 2, target src/encoding/json/decode.go
