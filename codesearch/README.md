# Introduction

This is a completely rewritten version of Russ Cox's [codesearch implementation](https://github.com/google/codesearch).

The primary differences are:

1. The original code stored the index in a custom binary format. This version keeps the index in [pebble](https://pkg.go.dev/github.com/cockroachdb/pebble) (a LSMT).

2. The original code was nice (and fast) but strongly coupled to trigrams. For performance reasons, each trigram was packed into a `uint32`. This made supporting other length ngrams difficult, and also meant there was a lot of `uint32` specific code implementing radix-sort and other operations for trigrams. This code introduces a Token interface and implementation backed by `[]byte`.

3. This code introduces interfaces for `Token`, `Tokenizer`, `IndexReader`, `IndexWriter`, `Field`, and `Document`. It stores posting lists as 64-bit roaring bitmaps. Where possible it removes fiddly bit-shifting or counting operations and uses methods from the standard library.

4. This code introduces the concept of fields, so document content from different fields is indexed in different posting lists. This makes it possible to search just one field (say, filename), without needing to iterate through all documents.

5. This code allows for custom tokenizers. This is to allow for more optimal indexing strategies in the future, like sparse-ngrams. EDIT: sparse ngrams are supported now.

6. This code replaces the custom `Query` object with s-expressions for all raw queries. The following grammar is supported:

```lisp
	(:eq fieldName "ngram") # Find matches for "ngram" in fieldName. Example: (:eq filename ".go")
	(:none)                 # Match nothing.
	(:all)                  # Match everything.
	(:and expr1 ... exprN)  # Find the intersection of matches from expr1 through exprN.
	(:or expr1 ... exprN)   # Find the union of matches from expr1 through exprN.
```

Some example queries:

```lisp
	(:and (:eq filename "ted") (:eq filename "dis"))
	(:and (:and (:eq content "=ba") (:eq content "foo") (:eq content "o=b") (:eq content "oo=")) (:or (:eq content "bar") (:eq content "baz")))
	(:all)
```

7. This code adds a gRPC Server implementation.

# Usage

## Index a directory (CLI)

```bash
  # Index the ~/buildbuddy directory.
  $ bazel build codesearch/cmd/cli && time bazel-bin/codesearch/cmd/cli/cli_/cli index --index_dir=/tmp/csindex ~/buildbuddy
```

## Search the index (CLI)

```bash
  # Search (w/ a regex)
  $ bazel build codesearch/cmd/cli && time bazel-bin/codesearch/cmd/cli/cli_/cli search --index_dir=/tmp/csindex "className=\.*"
```

## Run a codesearch server

```bash
  $ bazel run codesearch/cmd/server -- \
    --codesearch.index_dir=/tmp/csindex/ \
    --codesearch.scratch_dir=/tmp/csscratch \
    --codesearch.remote_cache=grpcs://remote.buildbuddy.dev \
    --auth.remote_auth_target=grpcs://remote.buildbuddy.dev \
    --auth.jwt_key=SET_THIS_TO_THE_DEV_JWT_KEY \
    --monitoring.listen=0.0.0.0:9999
```

## Index some code (server)

```bash
  $ grpc_cli call --metadata x-buildbuddy-api-key:YOUR_DEV_API_KEY localhost:2633 codesearch.service.CodesearchService.Index 'git_repo:<repo_url:"https://github.com/buildbuddy-io/buildbuddy"> repo_state:<commit_sha:"master">'
  #
  # grpcurl version:
  $ grpcurl -plaintext -H "x-buildbuddy-api-key:YOUR_DEV_API_KEY" -d '{"git_repo": {"repo_url":"https://github.com/buildbuddy-io/buildbuddy"}, "repo_state": {"commit_sha":"master"}}' localhost:2633 codesearch.service.CodesearchService.Index
```

## Perform a Search (server)

```bash
  $ grpc_cli call --metadata x-buildbuddy-api-key:YOUR_DEV_API_KEY localhost:2633 codesearch.service.CodesearchService.Search 'query: <term: "package codesearch">'
  #
  # grpcurl version:
  $ grpcurl -plaintext -H "x-buildbuddy-api-key:YOUR_DEV_API_KEY" -d '{"query": {"term": "package codesearch"}}' localhost:2633 codesearch.service.CodesearchService.Search
```

## Index Kythe annotations (server)

```bash
  # go find a recent workflow run of the "Generate CodeSearch Index" workflow on dev
  # and copy the digest of the `kythe_serving.sst` file from the `Artifacts` tab
  $ grpc_cli call --metadata x-buildbuddy-api-key:YOUR_DEV_API_KEY localhost:2633 codesearch.service.CodesearchService.IngestAnnotations 'sstable_name: <digest: <hash:"YOUR_HASH" size_bytes:100000> cache_type: CAS>'
  #
  # grpcurl version:
  $ grpcurl -plaintext -H "x-buildbuddy-api-key:YOUR_DEV_API_KEY" -d '{"sstable_name": {"digest": {"hash":"YOUR_HASH", "size_bytes":1000000}, "cache_type":"CAS"}}' localhost:2633 codesearch.service.CodesearchService.IngestAnnotations
```

## Fetch Kythe annotations (server)

```bash
  $ grpc_cli call --metadata x-buildbuddy-api-key:YOUR_DEV_API_KEY localhost:2633 codesearch.service.CodesearchService.KytheProxy 'decorations_request: <location: <ticket:"kythe://buildbuddy?path=proto/spawn_diff.proto"> references:true target_definitions:true semantic_scopes:true diagnostics:true>'
  #
  # grpcurl version:
  $ grpcurl -plaintext -H "x-buildbuddy-api-key:YOUR_DEV_API_KEY" -d '{"decorations_request":{"location": {"ticket":"kythe://buildbuddy?path=proto/spawn_diff.proto"}, "references":true, "target_definitions": true, "semantic_scopes": true, "diagnostics": true}}' localhost:2633 codesearch.service.CodesearchService.KytheProxy
```
