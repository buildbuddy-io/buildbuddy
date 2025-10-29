load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "migration_operators",
    srcs = ["migration_operators.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/batch_operator",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/rpcutil",
        "//server/util/status",
        "//server/util/usageutil",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "migration_operators_test",
    size = "small",
    srcs = ["migration_operators_test.go"],
    deps = [
        ":migration_operators",
        "//enterprise/server/batch_operator",
        "//proto:remote_execution_go_proto",
        "//proto:routing_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/status",
        "//server/util/usageutil",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)
