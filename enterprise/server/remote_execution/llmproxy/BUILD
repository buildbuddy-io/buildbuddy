load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "llmproxy",
    srcs = [
        "proxy.go",
        "service.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/llmproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/llmproxy/ports",
        "//proto:agent_security_go_proto",
        "//server/util/log",
        "//server/util/redact",
        "@com_github_klauspost_compress//zstd",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
