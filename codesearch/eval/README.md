# Search quality eval: codesearch vs cs.opensource.google

Compares our ranking against Google's public Code Search (cs.opensource.google)
on a fixed query set over the golang/go repo, and tracks target-file recall
(zoekt-style: each labeled query has one or more "right answer" files).

## Layout

The harness code lives in-tree; the bulky data (corpora, local indexes, the
zoekt clone) lives **outside the repo** at `$DATA` (default
`~/codesearch-eval`) so its vendored BUILD files don't break `//codesearch/...`
bazel builds. Nothing here is checked in yet.

In-tree:
- `searcheval.go` — the harness (`bb build //codesearch/eval:searcheval`).
- `queries.jsonl` / `queries_bazel.jsonl` — query sets in 6 categories
  (`symbol`, `keyword`, `phrase`, `regex`, `filter`, `ranking`). Labeled
  queries carry `targets`: acceptable answer files, repo-relative (e.g.
  `src/fmt/print.go`). Category design follows Sadowski et al. (FSE'15):
  short, identifier-heavy, known-item.
- `cache/google/`, `cache/google_bazel/` — one JSON file per query id holding
  Google's raw response. Kept on purpose: reruns never hit the network for
  cached queries; a cache entry is refetched only if its query text changes.
- `reports/`, `reports_bazel/` — `results.jsonl` (per-query detail) +
  `summary.md` (metric table).

Out-of-tree under `$DATA`:
- `corpus/go` — golang/go shallow clone (we indexed commit
  `3b00b08c44f87cb75ffbcabeb896cb4c0a74895d`); `corpus/bazel` — bazelbuild/bazel.
- `index_go`, `index_bazel` — the local codesearch indexes.
- `zoekt/` — the zoekt clone, patched CLI binary, ctags, and zoekt indexes.

## Setup (once)

```sh
DATA=~/codesearch-eval
git clone --depth 1 https://github.com/golang/go $DATA/corpus/go
bb build //codesearch/cmd/cli
# Index the module root (where go.mod lives), not the repo root: the indexer
# reads <dir>/go.mod to derive Go import identities, and golang/go's module
# ("std") is rooted at src/. Indexing $DATA/corpus/go instead leaves the
# module path empty, disabling import identities (and the import-rank signal).
bazel-bin/codesearch/cmd/cli/cli_/cli index \
  -index_dir $DATA/index_go -namespace eval -reset \
  $DATA/corpus/go/src
```

## Run

The harness shells out to the codesearch cli for local searches, so it
evaluates whatever `//codesearch/cmd/cli` is built from — rebuild that (and
re-index, if your change affects index-time behavior) to evaluate a change.

```sh
bb build //codesearch/cmd/cli //codesearch/eval:searcheval
EVAL=$PWD/codesearch/eval
DATA=~/codesearch-eval
bazel-bin/codesearch/eval/searcheval_/searcheval \
  -cli bazel-bin/codesearch/cmd/cli/cli_/cli \
  -index_dir $DATA/index_go -namespace eval \
  -queries $EVAL/queries.jsonl -cache_dir $EVAL/cache/google \
  -report_dir $EVAL/reports \
  -strip_prefix corpus/go/
```

Useful flags: `-cli_extra_args "-rescore_mode=gate"` (passed through to `cli
search`, e.g. to A/B a scoring knob; import-rank is always on and not
configurable), `-report_dir` (point different experiment runs at different
dirs to diff them), `-results 50`.

The harness is offline by default: a query with no cached Google response is
reported as an error. Pass `-allow_fetch` explicitly to fill the cache (e.g.
after adding new queries).

## Zoekt comparison (optional)

Pass `-zoekt`/`-zoekt_index_dir` to add zoekt columns to the report. Use
zoekt's **default** scoring (drop `-zoekt_extra_args`); it needs
universal-ctags on PATH (or `CTAGS_COMMAND`) to extract symbols, without which
its ranking collapses. Setup under `$DATA/zoekt/`:

```sh
DATA=~/codesearch-eval
git clone --depth 1 https://github.com/sourcegraph/zoekt $DATA/zoekt/src
# Local patch (kept in the clone): cmd/zoekt defaults to sourcegraph's
# SearchOptions and adds a -bm25 flag; re-apply if re-cloning.
cd $DATA/zoekt/src && go build -o ../bin/zoekt ./cmd/zoekt && go build -o ../bin/zoekt-git-index ./cmd/zoekt-git-index
# universal-ctags binary lives at $DATA/zoekt/bin/universal-ctags
CTAGS_COMMAND=$DATA/zoekt/bin/universal-ctags \
  $DATA/zoekt/bin/zoekt-git-index -index $DATA/zoekt/index -branches master $DATA/corpus/go
```

## Google API etiquette

The harness talks to the undocumented Grimoire endpoint backing
cs.opensource.google (no auth; the API key is the public one served in the
page HTML). Be polite:

- Every successful response is cached; only cache misses hit the network.
- 2.5s delay between requests (`-fetch_delay`), `-max_fetches` cap per run.
- On the first HTTP 429 the run stops fetching entirely; rerun later and the
  cache fills incrementally. 429 "Sorry" blocks clear after ~30–60 min.

## Metrics

- **recall@1/5/10, MRR** — over labeled queries, for both engines (Google's own
  numbers are the sanity baseline: if Google also misses a target, the label
  may be wrong or the query genuinely hard).
- **J@10** — Jaccard overlap of top-10 file sets (result-set agreement).
- **g@1∈L5** — how often Google's #1 result appears in our top 5 (cheap proxy
  for "would a user familiar with Google CS find our ranking sane").

Caveats: Google indexes master while we index a pinned commit (small drift);
Google's ranking is itself imperfect — treat it as a reference point, not
ground truth.
