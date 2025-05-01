# Overview

The `ociregistry` package contains an OCI registry server.
The main use of this registry server is as a mirror for Docker Hub
that caches image manifests and layers on read.

This package implements enough of the [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec)
to allow `docker pull ${IMAGE_NAME}[:${OPTIONAL_TAG_OR_DIGEST}]` to work:
* `GET /v2/`
* `HEAD /v2/[${OPTIONAL_REGISTRY_NAME}/]${REPOSITORY_NAME}/manifests/${TAG_OR_DIGEST}`
* `GET /v2/[${OPTIONAL_REGISTRY_NAME}/]${REPOSITORY_NAME}/manifests/${TAG_OR_DIGEST}`
* `HEAD /v2/[${OPTIONAL_REGISTRY_NAME}/]${REPOSITORY_NAME}/blobs/${DIGEST}`
* `GET /v2/[${OPTIONAL_REGISTRY_NAME}/]${REPOSITORY_NAME}/blobs/${DIGEST}`


## Terminology
* The [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec) refers to image layers as blobs.
* What people often call an "image name" is really an optional registry host (and optional port) plus a repository name. For example, `docker pull alpine` really means `docker pull index.docker.io/alpine:latest`, where `index.docker.io` is the registry, `alpine` is the repository, and `latest` is a tag.
* A digest looks like `sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef` (it contains both the hashing function and the hex string of the hash itself). The [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec) says that registries must support `sha256` and may support `sha512`.

## Caching
When serving a request for a manifest or layer, the registry server will first consult the cache. If it finds what it's looking for, it will respond with data pulled from the cache. If not, it will make a request to the upstream registry, and write the data it finds to the cache.

The registry server stores data in both the action cache (AC) and the content addressable store (CAS).

### Current cache organization for public images

* The manifest or layer contents are stored as a blob in the CAS, to allow the registry to serve `GET` requests. The manifest or layer digest is the same for the CAS and for the upstream registry.
* Metadata about the manifest or layer (the content length, plus the HTTP `Content-Type` header value returned from the upstream registry) are stored
in a proto in the CAS. This metadata is stored separately from the contents so that the registry can serve `HEAD` requests without fetching contents.
* An ActionResult pointing to the CAS blob and the CAS metadata proto is stored in the AC. The key is a hash of `{registry, repository, blob|manifest, hash function, hash hex string}`. The registry server has this information when serving a request (or can find it out from the upstream).

If any of the above three entries are missing, the registry server considers it a cache miss.

### Future cache organization for public and private images

Manifest contents are usually small enough to fit in the AC. So we're moving to a single AC entry for manifests:
* An ActionResult containing the manifest `Content-Type` header value and the manifest contents as execution metadata. The key is still a hash of `{registry, repository, blob|manifest, hash function, hash hex string}`.
