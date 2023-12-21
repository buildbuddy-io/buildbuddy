load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testauditlog",
    srcs = ["testauditlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testauditlog",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:auditlog_go_proto",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)
