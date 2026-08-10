load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "useragent",
    srcs = ["useragent.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/useragent",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_grpc//metadata"],
)

go_test(
    name = "useragent_test",
    srcs = ["useragent_test.go"],
    deps = [
        ":useragent",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
