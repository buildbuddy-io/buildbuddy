load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "workspace",
    srcs = [
        "workspace.go",
        "workspace_unix.go",
        "workspace_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace",
    deps = [
        "//enterprise/server/cmd/ci_runner/bundle",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/overlayfs",
        "//enterprise/server/remote_execution/vfs",
        "//enterprise/server/util/ci_runner_util",
        "//enterprise/server/util/vfs_server",
        "//proto:remote_execution_go_proto",
        "//server/cache/dirtools",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/fspath",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_gobwas_glob//:glob",
        "@io_opentelemetry_go_otel//attribute",
        "@org_golang_x_sync//errgroup",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@com_github_google_uuid//:uuid",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_google_uuid//:uuid",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "workspace_test",
    size = "small",
    srcs = ["workspace_test.go"],
    deps = [
        ":workspace",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/fspath",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
