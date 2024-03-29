# Introduction

This is a completely rewritten version of Russ Cox's [codesearch implementation](https://github.com/google/codesearch).

The primary differences are:

1. The original code stored the index in a custom binary format. This version keeps the index in [pebble](https://pkg.go.dev/github.com/cockroachdb/pebble) (a LSTM).

2. The original code was nice (and fast) but strongly coupled to trigrams. For performance reasons, each trigram was packed into a `uint32`. This made supporting other length ngrams difficult, and also meant there was a lot of `uint32` specific code implementing radix-sort and other operations for trigrams. This code introduces a Token interface and implementation backed by `[]byte`.

3. This code introduces interfaces for `Token`, `Tokenizer`, `IndexReader`, `IndexWriter`, `Field`, and `Document`. It stores posting lists as 64-bit roaring bitmaps. Where possible it removes fiddly bit-shifting or counting operations and uses methods from the standard library.

4. This code introduces the concept of fields, so document content from different fields is indexed in different posting lists. This makes it possible to search just one field (say, filename), without needing to iterate through all documents.

5. This code allows for custom tokenizers. This is to allow for more optimal indexing strategies in the future, like sparse-ngrams.

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
  $ blaze build codesearch/cmd/cindex:cindex && time bazel-bin/codesearch/cmd/cindex/cindex_/cindex ~/buildbuddy
```

## Search the index (CLI)

```bash
  # Search (w/ a regex)
  $ blaze build codesearch/cmd/csearch:csearch && time bazel-bin/codesearch/cmd/csearch/csearch_/csearch -verbose "className=\.*"
```
