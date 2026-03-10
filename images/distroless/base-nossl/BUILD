# Adapted from the example at https://github.com/bazel-contrib/rules_img/tree/main/e2e/generic/custom_distroless_base_image

load("@rules_distroless//distroless:defs.bzl", "flatten")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "layer_from_tar")
load("@rules_img//img:load.bzl", "image_load")

package(default_visibility = ["//visibility:public"])

[
    flatten(
        name = "tar_%s" % arch,
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
        tars = [
            "@debian13//libc6/%s" % arch,
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

[
    layer_from_tar(
        name = "layer_%s" % arch,
        src = "tar_%s" % arch,
        annotations = {
            "org.opencontainers.image.title": "libc (debian13)",
            "org.opencontainers.image.description": (
                "Provides debian13 libc"
            ),
            "org.opencontainers.image.vendor": "Buildbuddy",
        },
        compress = "gzip",
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

# Create a complete distroless static image with all features
image_manifest(
    name = "manifest",
    base = "//images/distroless/static:manifest",
    layers = select({
        "@platforms//cpu:arm64": [":layer_arm64"],
        "@platforms//cpu:x86_64": [":layer_amd64"],
    }),
    user = "root",
)

image_index(
    name = "index",
    manifests = [":manifest"],
    platforms = [
        "//platforms:linux_x86_64",
        "//platforms:linux_arm64",
    ],
    target_compatible_with = select({
        # the cacerts target referenced
        # from here currently doesn't
        # support running on Windows
        "@platforms//os:windows": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
)

# Load a multi-platform image
image_load(
    name = "load",
    # Explicitly use containerd for clarity. The container will be accessible
    # via `nerdctl` in the "moby" namespace.
    #
    # We use containerd, as advised by `rules_img`, to avoid `docker load`,
    # which, if the host system is not using the containerd backend for docker,
    # will create a custom docker manifest.
    # Loading into containerd allows us to load the exact OCI image we've built
    # with the expected digest.
    daemon = "containerd",
    image = ":index",  # References an image_index
    tag = "bb-distroless_base-nossl:latest",
)
