load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "quota",
    srcs = ["quota_manager.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/quota",
    deps = [
        "//enterprise/server/backends/pubsub",
        "//proto:quota_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/alert",
        "//server/util/db",
        "//server/util/log",
        "//server/util/query_builder",
        "//server/util/quota",
        "//server/util/status",
        "@com_github_throttled_throttled_v2//:throttled",
        "@com_github_throttled_throttled_v2//store/goredisstore.v8:goredisstore_v8",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

go_test(
    name = "quota_test",
    size = "small",
    srcs = ["quota_manager_test.go"],
    embed = [":quota"],
    deps = [
        "//enterprise/server/backends/userdb",
        "//proto:quota_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/testutil/pubsub",
        "//server/testutil/testenv",
        "//server/util/query_builder",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_cmp//cmp/cmpopts",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
