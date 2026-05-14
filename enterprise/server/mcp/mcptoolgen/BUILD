load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "mcptoolgen_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/mcptoolgen",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/mcp/mcptools",
        "//proto/api/v1:api_v1_go_proto",
        "@com_github_jhump_protoreflect//desc/protoparse",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//types/descriptorpb",
    ],
)

go_binary(
    name = "mcptoolgen",
    data = ["//proto/api/v1:api_v1_proto_srcs"],
    embed = [":mcptoolgen_lib"],
    visibility = ["//visibility:public"],
)
