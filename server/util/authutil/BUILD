load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "authutil",
    srcs = ["authutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/authutil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/blocklist",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_rpc//errdetails",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "authutil_test",
    size = "small",
    srcs = ["authutil_test.go"],
    embed = [":authutil"],
    deps = [
        "@com_github_stretchr_testify//assert",
    ],
)
