load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "relaywire",
    srcs = ["relaywire.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/relaywire",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:gateway_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "relaywire_test",
    srcs = ["relaywire_test.go"],
    embed = [":relaywire"],
    deps = [
        "//proto:gateway_go_proto",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
