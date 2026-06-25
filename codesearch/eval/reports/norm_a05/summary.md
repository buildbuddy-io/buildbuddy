# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 25 | 25 | 32% | 88% | 0.54 | 80% | 96% | 0.88 | 40% | 72% | 0.53 | 0.20 | 80% |
| keyword | 20 | 20 | 15% | 50% | 0.34 | 70% | 90% | 0.78 | 50% | 90% | 0.67 | 0.29 | 35% |
| phrase | 15 | 15 | 20% | 67% | 0.40 | 40% | 80% | 0.57 | 53% | 87% | 0.66 | 0.26 | 47% |
| regex | 15 | 6 | 50% | 67% | 0.57 | 50% | 83% | 0.62 | 0% | 50% | 0.23 | 0.10 | 13% |
| filter | 10 | 5 | 60% | 100% | 0.75 | 60% | 100% | 0.80 | 40% | 60% | 0.45 | 0.41 | 60% |
| ranking | 15 | 0 | - | - | - | - | - | - | - | - | - | 0.04 | 7% |
| **total** | 100 | 71 | 28% | 72% | 0.47 | 65% | 90% | 0.76 | 42% | 77% | 0.57 | 0.21 | 43% |

## Queries where we miss the target but Google finds it

- `QuoteMeta` (sym-quotemeta): local rank 43, google rank 1, target src/regexp/regexp.go
- `func Pipe` (sym-pipe): local rank 10, google rank 2, target src/io/pipe.go
- `hmac New` (sym-hmac): local rank 14, google rank 2, target src/crypto/hmac/hmac.go
- `bytes buffer grow` (kw-bytes-buffer-grow): local rank 6, google rank 1, target src/bytes/buffer.go
- `context cancel` (kw-context-cancel): local rank 29, google rank 1, target src/context/context.go
- `tls handshake client` (kw-tls-handshake-client): local rank 8, google rank 1, target src/crypto/tls/handshake_client.go
- `gzip reader` (kw-gzip-reader): local rank 8, google rank 2, target src/compress/gzip/gunzip.go
- `template parse tree` (kw-template-parse-tree): local rank 21, google rank 1, target src/text/template/parse/parse.go
- `utf8 decode rune` (kw-utf8-decode-rune): local rank 19, google rank 1, target src/unicode/utf8/utf8.go
- `mutex starvation` (kw-mutex-starvation): local rank 6, google rank 1, target src/sync/mutex.go
- `http client redirect` (kw-http-client-redirect): local rank 8, google rank 1, target src/net/http/client.go
- `sendfile` (kw-sendfile): local rank miss, google rank 1, target src/net/sendfile_unix_alt.go
- `"concurrent map read and map write"` (phr-concurrent-map): local rank 11, google rank 1, target src/runtime/map_noswiss.go
- `"context deadline exceeded"` (phr-deadline-exceeded): local rank 8, google rank 1, target src/context/context.go
- `EOF = errors\.New` (re-eof-error): local rank 9, google rank 1, target src/io/io.go
