"""The podman version used in tests and distributed with the buildbuddy executor image.

When changing this version, a new release of podman-static may be needed.
See dockerfiles/executor_image/README.md for instructions.
The checksums below will also need to be updated.
"""
PODMAN_VERSION = "v4.9.4"

# These SHA256 checksums also need to be updated when bumping the podman version
# (they are the checksums of the .tar.gz archives).

PODMAN_STATIC_SHA256_AMD64 = "ac14152d2ef5cb25abbd615893cd56355f951f075a7ae663f0b2c1bc5d3fb77e"
PODMAN_STATIC_SHA256_ARM64 = "72ab4a9c6ae9d5d00200070cb43b0921117898925a284bfacbfb3873aa82e598"
