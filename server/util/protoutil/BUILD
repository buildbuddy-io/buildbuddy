load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "protoutil",
    srcs = ["protoutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/protoutil",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_protobuf//proto"],
)

go_test(
    name = "protoutil_test",
    srcs = ["protoutil_test.go"],
    deps = [
        ":protoutil",
        "//proto:distributed_cache_go_proto",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
