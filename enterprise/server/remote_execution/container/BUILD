load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "container",
    srcs = ["container.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container",
    deps = [
        "//enterprise/server/remote_execution/block_io",
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/unixcred",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "container_test",
    size = "small",
    srcs = ["container_test.go"],
    deps = [
        ":container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/timeseries",
        "@com_github_google_go_cmp//cmp",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_x_sync//errgroup",
    ],
)
