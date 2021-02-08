load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/auth",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:context_go_proto",
        "//proto:user_id_go_proto",
        "//server/interfaces:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/status:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
    ],
)
