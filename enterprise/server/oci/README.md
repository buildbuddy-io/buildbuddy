# oci packages

The packages underneath this directory implement fetching OCI blobs and manifests from remote registries,
writing to OCI image layers to disk, writing OCI blobs to (and reading from) a byte stream server, and
writing OCI manifests to (and reading from) the action cache.

Currently this functionality lives in a variety of packages:
- [//enterprise/server/ociregistry](../ociregistry) provides an HTTP mirror for Docker Hub that caches blobs and manifests on read.
- [//enterprise/server/remote_execution/containers/ociruntime](../remote_execution/containers/ociruntime) has functions for writing OCI image layer
* [//enterprise/server/util/oci](../util/oci) fetches blobs and manifests (caching on read) from remote registries and returning
[go-containerregistry](https://github.com/google/go-containerregistry) `Image`s.
- [//enterprise/server/util/ocicache](../util/ocicache) provides functions for writing to and reading from the action cache and a
byte stream server.
- [//enterprise/server/util/ociconv](../util/ociconv) converts OCI image layer tarballs to ext4 filesystems.
tarballs to disk.
