# oci packages

## Current state of the world

Libraries for fetching, caching, mirroring, and materializing OCI images are currently spread across multiple packages:

- [//enterprise/server/ociregistry](../ociregistry) provides an HTTP mirror for Docker Hub that caches blobs and manifests on read.
- [//enterprise/server/remote_execution/containers/ociruntime](../remote_execution/containers/ociruntime) has functions for writing OCI image layer tarballs to disk.
- [//enterprise/server/util/oci](../util/oci) fetches blobs and manifests (caching on read) from remote registries and returning
  [go-containerregistry](https://github.com/google/go-containerregistry) Images.
- [//enterprise/server/util/ocicache](../util/ocicache) provides functions for writing to and reading from the action cache and a
  byte stream server.
- [//enterprise/server/util/ociconv](../util/ociconv) converts OCI image layer tarballs to ext4 filesystems.

## End goal

Eventually, all libraries for working with OCI images should live under this package.

To start, the [ocifetcher](ocifetcher) package, which provides a client for fetching OCI blobs and manifests from remote registries, lives here.
