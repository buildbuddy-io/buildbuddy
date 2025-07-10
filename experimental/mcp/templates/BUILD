load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "templates",
    srcs = ["templates.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/templates",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:target_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/environment",
        "//server/util/status",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

package(default_visibility = ["//experimental:__subpackages__"])
