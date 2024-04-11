load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "podman",
    srcs = ["podman.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman",
    deps = [
        "//enterprise/server/remote_execution/cgroup",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/soci_store",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/networking",
        "//server/util/random",
        "//server/util/retry",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_masterminds_semver_v3//:semver",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "podman_test",
    srcs = ["podman_test.go"],
    deps = [
        ":podman",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

# Repackages the podman-static distribution to make it more easily consumable
# both by the container_layer rule and by tests:
#
# - Replaces the registry config with our custom one. This way, we don't have to
#   override it separately.
# - Strips the top-level podman-static directory, so that the archive will contain
#   paths like ./usr/bin/podman rather than ./podman-static/usr/bin/podman.
#   This is needed because the 'tars' attribute of container_layer does not seem to
#   support stripping components, currently.
genrule(
    name = "podman-static",
    srcs = select({
        "@platforms//cpu:x86_64": ["@com_github_buildbuddy_io_podman_static_podman-linux-amd64//file"],
        "@platforms//cpu:aarch64": ["@com_github_buildbuddy_io_podman_static_podman-linux-arm64//file"],
    }) + [
        "//dockerfiles/executor_image:data/registries.conf",
    ],
    outs = ["podman-static.tar.gz"],
    cmd_bash = """
        TMPDIR=$$(mktemp -d)
        trap 'rm -rf "$$TMPDIR"' EXIT
        SRCS=($(SRCS))
        PODMAN_ARCHIVE=$${SRCS[0]}
        REGISTRIES_CONF=$${SRCS[1]}
        tar --extract --file="$$PODMAN_ARCHIVE" --strip-components=1 --directory="$$TMPDIR"

        # Remove README.md (not needed)
        rm "$$TMPDIR/README.md"

        # Replace /etc/containers/registries.conf with our custom config.
        rm "$$TMPDIR/etc/containers/registries.conf"
        cp "$$REGISTRIES_CONF" "$$TMPDIR/etc/containers/registries.conf"

        tar --create --gzip --file="$@" --directory="$$TMPDIR" .
    """,
)
