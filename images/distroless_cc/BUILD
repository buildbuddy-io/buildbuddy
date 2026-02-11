load("@rules_distroless//distroless:defs.bzl", "cacerts", "flatten", "group", "java_keystore", "passwd")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:push.bzl", "image_push")

package(default_visibility = ["//visibility:public"])

# Export the apt lockfile and manifest for the module extension
exports_files([
    "apt.lock.json",
    "apt.yaml",
])

[
    flatten(
        name = "cc_layer_%s" % arch,
        compress = "gzip",
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:%s" % {
                "amd64": "x86_64",
                "arm64": "aarch64",
            }[arch],
        ],
        tars = [
            "@bookworm//:libgomp1",
            "@bookworm//:libstdc++6",
            "@bookworm//:libgcc-s1",
            "@bookworm//:gcc-14-base",
            "@bookworm//:dpkg_status",
        ],
    )
    for arch in [
        "amd64",
        "arm64",
    ]
]

# Create a minimal Python distroless image
image_manifest(
    name = "cc_manifest",
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    },
    layers = select({
        "@platforms//cpu:arm64": [
            ":base_layer_arm64",
            ":python_layer_arm64",
        ],
        "@platforms//cpu:x86_64": [
            ":base_layer_amd64",
            ":python_layer_amd64",
        ],
    }),
    user = "root",
)

image_index(
    name = "python_distroless",
    manifests = [":python_manifest"],
    platforms = [
        "//platform:linux_amd64",
        "//platform:linux_arm64",
    ],
    target_compatible_with = select({
        # the cacerts target referenced
        # from here currently doesn't
        # support running on Windows
        "@platforms//os:windows": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
)
