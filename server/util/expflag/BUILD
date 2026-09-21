load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "expflag",
    srcs = ["expflag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/expflag",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:experiments_go_proto",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "@org_golang_google_protobuf//types/known/structpb",
    ],
)

go_test(
    name = "expflag_test",
    srcs = ["expflag_test.go"],
    deps = [
        ":expflag",
        "//proto:experiments_go_proto",
        "//server/interfaces",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_google_protobuf//types/known/structpb",
    ],
)
