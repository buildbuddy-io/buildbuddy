load("@rules_distroless//distroless:defs.bzl", "flatten")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:load.bzl", "image_load")

package(default_visibility = ["//visibility:public"])

[
    flatten(
        name = "layer_%s" % arch,
        compress = "gzip",
        tags = ["manual"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
        tars = [
            "//images/distroless/static:layer_%s" % arch,
            "@trixie//libc6:libc6",

            # These are included in the distroless base image for debian, but
            # not *currently* needed for buildbuddy. We can uncomment these if
            # we start depending on them.

            # "@trixie//libssl3t64/%s:data" % arch,
            # "@trixie//libzstd1/%s:data" % arch,
            # "@trixie//zlib1g/%s:data" % arch,
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
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
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
    daemon = "docker",  # Explicitly use docker
    image = ":index",  # References an image_index
    tag = "bb-distroless_base-nossl:latest",
)
