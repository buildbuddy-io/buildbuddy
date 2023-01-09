load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "redact",
    srcs = ["redact.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/redact",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//server/environment",
        "//server/util/flagutil",
        "//server/util/git",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)

go_test(
    name = "redact_test",
    size = "small",
    srcs = ["redact_test.go"],
    deps = [
        ":redact",
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
