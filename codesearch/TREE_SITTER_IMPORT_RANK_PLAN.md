# Tree-sitter Import Annotations + Rank (MVP)

## Goal

Flag-gated tree-sitter extraction at indexing time: parse each file as it is
indexed, store what it imports as fields on the document, and let the index's
own posting lists serve as the reverse-import graph. Use in-degree (how many
files import this file's package) as a conservative ranking boost — a cheap
precursor to pagerank-style weighting. Long-term, the same per-doc annotation
pattern grows into the replacement for kythe (which doesn't run today and
will be removed).

## The central idea

A document can only carry data computable from that document alone — forward
imports, not the aggregate "who imports me." But if imports are indexed as a
`KeywordField`, the index machinery already maintains the aggregate: the
posting list for term P in the `imports` field enumerates the docs importing
P, and its cardinality **is** the in-degree. No side tables, no separate
build step, no staleness: the graph is index data and inherits the index's
incremental updates, deletes, tenancy, and lifecycle for free.

## Schema changes (`GitHubFileSchema`)

- `imports` — `KeywordField`, indexed + stored. Space-joined resolved import
  identities of packages/files this doc imports (deduped, external imports
  and self-edges dropped). Whitespace tokenizer → one term per import.
- `import_id` — `KeywordField`, indexed + stored. This file's own import
  identity (Go: `<module>/<package dir>`). Empty for files that should not
  receive boost (e.g. `_test.go`). May be multi-valued for languages where a
  file is importable under several names (JS `./foo` vs `./foo/index.js`).

Both fields are additive; docs indexed before the flag simply lack them
(missing `imports` under-counts others' in-degree, missing `import_id` means
no boost for that doc — both heal on reindex; no backfill tool).

## Extraction (new package `codesearch/importgraph`)

- Per-language `Extractor` interface: `Language() string;
  Extract(path string, content []byte, rctx RepoContext) (imports []string, importID string)`.
  Dispatch on the enry-detected language already computed in
  `AddFileToIndex`. Parse failures: log, count, return nothing — **never
  fail indexing**.
- **MVP language: Go** (grammar `smacker/go-tree-sitter/golang` — dep already
  in go.mod and building under bazel via `cli/fix/typescript`). Handles
  factored/aliased/blank imports; dedupes; drops imports outside the module.
- `RepoContext` carries repo-level inputs, for Go just the module path:
  - CLI: read `go.mod` from the directory before walking.
  - Server full reindex: read the `go.mod` entry from the zip archive first
    (central directory makes this cheap), and store the module path in the
    repo metadata doc.
  - Incremental updates: read module path from the repo metadata doc
    (re-parse and update it when a commit touches `go.mod`).
- Package-level identity sidesteps file-list fan-out: an import of package P
  boosts every non-test file whose `import_id` is P — matching the intended
  "imported package boosts its files" semantics without knowing the file
  list at extraction time.

## Indexing integration

- `github.AddFileToIndex` (flag-gated, e.g. `--codesearch.extract_imports` /
  CLI `-extract_imports`): after language detection, run the extractor and
  add the two fields to the doc before `AddDocument`/`UpdateDocument`.
  Per-file tree-sitter parse cost is paid here, while content is in memory.
- No separate rank/build command. `cli squery '(:eq imports <id>)'` already
  serves as the graph debugging tool.

## Search scoring: signals + boost functions

Shape borrowed from Solr function queries / ES `function_score`:
`final = textScore × boost(signals)`, where signals are per-doc values
available for **every match** (like term frequencies), not just a reranked
top-K.

- **Scoring-signals record**: generalize the per-doc stats record (`sta`
  key) — which today holds BM25 field lengths and is already loaded for
  every match by `populateFieldLengths` in `RawQuery` — to also carry
  `import_id` terms (and future signals: file size, path depth, …). Written
  at `AddDocument` from the same doc; read by the same per-match scan.
- **Reader resolves, Scorer computes**: after loading stats, the Reader
  resolves `import_id` → in-degree via memoized posting-list cardinality
  lookups (`imports` field) and attaches *numeric* signals to each match.
  `DocumentMatch` gains `Signal(name string) float64` alongside
  `FieldLength`. The `types.Scorer` contract (pure over index-side data) is
  unchanged — the signal set just widens. Resolution is lazy: scorers
  declare required signals, so filter-only / ranking-off queries pay
  nothing.
- **Boost function**: composable in Go —
  `boost = 1 + alpha * min(1, log2(1+indegree)/20)` (`alpha` flag, default
  0.25; fixed scale, no global state, stable under incremental updates).
  Adding a signal = a resolver + a formula term. A user-configurable
  expression language (CEL-style, à la Solr `bf`/`boost`) is a follow-up —
  the evaluation point will already exist.
- **Exact top-N**: every match is scored with full signals; no
  over-query/rerank phase.
- Costs: cardinality resolution is memoized per query and bounded by unique
  packages among matches; hot packages mean larger posting-list unmarshals.
  If profiles complain: cached count table (optional follow-up, not a
  redesign). Deleted docs inflate cardinality until `CompactDeletes` —
  acceptable for a fuzzy signal.

## Interaction with iterative indexing

This design makes the graph self-maintaining:

- Incremental updates (`ProcessCommit` → `UpdateDocument`) re-extract a
  changed file's imports as a side effect of reindexing it; posting lists
  are updated by the existing index machinery.
- Deletes flow through the existing tombstone + `CompactDeletes` path.
- No rebuild step, no staleness window beyond the index's own, no docid
  issues (rank is derived from terms, not doc identity).

## Relationship to Kythe: replacement, not fallback

Long-term: **remove kythe entirely** and rely on tree-sitter as the only
code-intelligence layer. Kythe requires compilation extraction and **does not
run today** — the baseline for code navigation is *nothing*. Tree-sitter
needs only file contents, no build integration. GitHub ships tag-based
navigation as its default, so syntactic-only nav is a proven product bar.

- **Nothing here consumes kythe data.** Kythe deletion is ungated dead-code
  removal (ingestion RPC, CI extraction, `kythestorage`, the `kythe.io`
  dependency tree), independent of any tree-sitter milestone.
- The per-doc annotation pattern extends to nav: defs/refs later become
  additional doc fields (stored span data + indexed symbol keyword fields),
  which is exactly how search-based code nav works — symbol lookup is an
  index query.
- **What tree-sitter nav will and won't do** (expectation-setting; these
  features are dark today): works — decorations/click-through, defs/refs,
  identifier search, doc comments. Approximable — call hierarchy,
  syntactically visible `extends`. Out of reach — Go interface `satisfies`,
  override resolution, `generates` edges.

### Wire contract for future nav (from `enterprise/app/code/kythe.tsx`, the only consumer)

Serving the existing kythe proto shapes is a thin shim, not a kythe
reimplementation. The UI calls only 4 of 7 `KytheRequest` branches —
`decorations`, `crossReferences`, `extendedXrefs`, `docs` (drop
`nodes`/`corpusRoots`/`directory`, no callers). It consumes: anchor spans
(line/column/byte offsets + snippet); opaque `targetTicket`s echoed back
verbatim (encode whatever we want); anchor parent tickets parsed only for
`?path=`; 5 edge-kind strings used as an enum; `nodeInfo.facts` with
`/kythe/node/kind` + `/kythe/subkind`. Only generated proto *types* are
needed — no kythe libraries. One frontend fix required: `fetchDecorations`
mints tickets as `kythe://buildbuddy?path=<file>` with no repo/commit; the
code browser knows both and must include them.

## Test plan

- Extractor unit tests: Go imports (single, factored block, aliased, blank,
  comments), `import_id` derivation, `_test.go` exclusion, parse-error
  tolerance, external-import filtering.
- Schema/index test: index a small synthetic repo with the flag on, assert
  `imports`/`import_id` stored fields and posting lists (via squery).
- Search test: two files matching a term equally; the one whose package is
  imported everywhere ranks first with `-use_ranks`, order unchanged
  without it.
- Incremental test: update a doc to add/remove an import; assert cardinality
  changes without any rebuild step.
- Quality check: `test/quality` rater before/after on a Go corpus
  (buildbuddy itself; kernel corpus is C — no change until the C extractor).
- `bb test codesearch/...`

## Import identity per language

`import_id` = the set of strings an importer could write (after
normalization) to designate this file.

**Term format** (VName semantics at keyword-term weight):
`<family>:<scope-qualified identity>`. The family prefix (`go:`, `java:`,
`js:`, `c:`) separates language vocabularies sharing the one `imports`
field — families, not languages (TS/JS share `js:`; C/C++ share `c:`).
Identities that are repo-local (file paths: js, c) are additionally
qualified with the repo; identities that are globally meaningful (Go module
paths, Java packages) are not, so legitimate cross-repo imports within a
multi-repo namespace still join while same-path files in different repos
don't collide. Both sides of the join are produced by the same extractor, so
the convention lives in one place. Decided now because changing term format
later breaks the join between old and new docs until a full reindex. When
defs/refs arrive, symbol identities extend the same scheme (and wire-level
tickets, opaque to the frontend, can wrap these strings verbatim).

Per language, normalization happens importer-side when resolvable from the
importing file alone, identity-side (multi-valued) when not:

- **Go** (MVP): directory-based —
  `go:<module path>/<package dir>`, one term, shared by all non-test files
  in the package. Importers' source strings are already canonical. Needs
  `go.mod` via `RepoContext`. Unqualified by repo: module paths are global,
  cross-repo imports join.
- **Java**: self-declared, no repo context —
  `java:<package decl>.<filename stem>` plus the bare package term
  `java:<package decl>`, so `import com.foo.bar.Baz;` joins the first and
  `import com.foo.bar.*;` joins the second (boosting the whole package).
  Unqualified: package names are global.
- **JS/TS**: importer resolves relative specifiers against its own dir; both
  sides drop extensions. `src/utils/foo.ts` → `js:<repo>:src/utils/foo`;
  `foo/index.ts` also emits the directory form `js:<repo>:foo`.
  Repo-qualified: paths are repo-local. Bare specifiers are external
  (dropped); tsconfig path aliases via `RepoContext` later.
- **C/C++**: quoted includes resolve importer-side (relative to includer's
  dir → repo-relative path); angle includes stay verbatim and the identity
  side emits directory-boundary path suffixes
  (`c:<repo>:include/linux/sched.h` → also `c:<repo>:linux/sched.h`,
  `c:<repo>:sched.h`). Repo-qualified. Bare-filename suffix collisions
  inflate counts — acceptable for a fuzzy signal, drop if noisy. Note: only
  headers accumulate in-degree, so C ranking favors headers (visible in
  kernel-corpus evals; arguably correct).

## Roadmap after the MVP

1. **More languages**: C `#include` (kernel corpus), then JS/TS/Python/Java —
   each is just a new `Extractor` implementing the identity scheme above.
2. **`imports:` search filter** — the field is already indexed; expose it as
   a query atom in `filters.go`.
3. **Configurable scoring formula**: expose the boost function as a
   user/operator-settable expression (CEL or similar) over named signals,
   Solr-`bf` style.
4. **Real pagerank**: forward edges are in the stored `imports` field;
   compute iteratively offline and store scores (would reintroduce a small
   side table — only if in-degree proves insufficient).
5. **Symbols**: defs/refs as additional doc fields via `tags.scm`-style tag
   queries (stored spans + indexed symbol keywords).
6. **Serve nav endpoints** from doc annotations, per the wire contract above.
   Note: nav endpoints lack a namespace today; tickets minted by the
   frontend must carry repo+commit (and effectively namespace) — design this
   when that phase starts.
7. **Server-side enablement**: turn the extraction flag on in production
   indexing; signal-based scoring in server search.
