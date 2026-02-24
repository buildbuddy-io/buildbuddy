load("@rules_distroless//distroless:defs.bzl", "cacerts", "flatten", "group", "java_keystore", "passwd")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:load.bzl", "image_load")

package(default_visibility = ["//visibility:public"])

exports_files([
    "custom-ca.crt",
    "internal-root.crt",
])

# Create group entries (similar to groupadd)
group(
    name = "groups",
    entries = [
        # Root group
        dict(
            name = "root",
            gid = 0,
            users = ["root"],
        ),
    ],
)

# Create passwd entries (similar to useradd/passwd)
passwd(
    name = "passwd",
    entries = [
        # Root user
        dict(
            gecos = ["root"],
            gid = 0,
            home = "/root",
            shell = "/bin/bash",
            uid = 0,
            username = "root",
        ),
    ],
)

# Generate CA certificates bundle
cacerts(
    name = "ca_certificates",
    package = "@trixie//ca-certificates/amd64:data",
    # the cacerts rule currently doesn't support running on Windows
    target_compatible_with = select({
        "@platforms//os:windows": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
)

# Create a Java keystore with custom certificates
java_keystore(
    name = "custom_java_keystore",
    certificates = [
        ":custom-ca.crt",
        ":internal-root.crt",
    ],
)

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
            "@trixie//base-files:base-files",
            "@trixie//netbase:netbase",
            "@trixie//tzdata:tzdata",
            "@trixie//media-types:media-types",
            ":groups",
            ":passwd",
            ":ca_certificates",
            ":custom_java_keystore",
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
    daemon = "docker",  # Explicitly use docker
    image = ":index",  # References an image_index
    tag = "bb-distroless_static:latest",
)
