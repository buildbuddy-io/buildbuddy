# oci packages

## How image fetching is organised

There is one implementation of OCI manifest and blob fetching,
[//enterprise/server/oci/ocifetch](ocifetch). A `Fetcher` runs one sequence:
consult the `Store`, go to the `Upstream`, write back to the `Store`. It is
parameterised only by those two dependencies:

- `Upstream` is where content comes from on a miss: `RegistryUpstream` (a
  container registry over HTTPS, with the puller cache, token-refresh retry,
  registry mirrors, private-IP allowlist and blob HEAD fallback) or
  `RemoteFetcherUpstream` (another OCIFetcher service).
- `Store` is where content is cached: `CacheStore` over the ActionCache and
  ByteStream services using the key layout in [ocicache](ocicache), a
  blob-only local store for the cache proxy, or nil for no caching.

Every host is the same library with different dependencies:

| Host | Upstream | Store |
|---|---|---|
| App OCIFetcher service (`ocifetcher.NewAppServer`) | registry | remote cache |
| Cache proxy OCIFetcher service (`ocifetcher.NewProxyServer`) | app's OCIFetcher service | proxy's local ByteStream, blobs only |
| Executor, OCI fetcher on | OCIFetcher service on the cache target | none |
| Executor, OCI fetcher off, authenticated | registry | remote cache |
| Executor, OCI fetcher off, anonymous | registry | none |

The executor's three rows are chosen in one place, `Resolver.fetcherFor` in
[//enterprise/server/util/oci](../util/oci). Warmup and tasks construct their
fetcher the same way.

Access proofs, singleflight for concurrent blob fetches, and the
`bypass_registry` rule (serve from the store only; a registry upstream refuses
contact, a remote fetcher upstream forwards the flag) live in the library, so
they behave the same wherever it runs.

## Packages

- [ocifetch](ocifetch): the fetching library described above, plus
  `go-containerregistry` `Image` and `Layer` views over it.
- [ocifetcher](ocifetcher): the OCIFetcher gRPC service, a thin wrapper over a
  `Fetcher` that parses references, authorises `bypass_registry` and adapts
  the stream. The same server type runs on the app and on the cache proxy.
- [ocicache](ocicache): the action cache and byte stream key layout for OCI
  manifests, blob metadata rows and blob bytes.
- [ociregistry](ociregistry): the HTTP registry mirror (`*.bbcr.io`) for
  docker clients. It caches through `ocicache` with its own fetch code; moving
  it onto `ocifetch` is a follow-up.
- [ociconv](ociconv): converts OCI image layer tarballs to ext4 filesystems.
- [//enterprise/server/remote_execution/containers/ociruntime](../remote_execution/containers/ociruntime)
  writes OCI image layer tarballs to disk.
