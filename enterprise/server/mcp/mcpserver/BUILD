load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "api_tools_manifest",
    srcs = ["//proto/api/v1:api_v1_proto_srcs"],
    outs = ["api_tools_manifest.json"],
    cmd = "$(location //enterprise/server/mcp/mcptoolgen:mcptoolgen) -output \"$@\"",
    tools = ["//enterprise/server/mcp/mcptoolgen"],
)

go_library(
    name = "mcpserver",
    srcs = ["mcpserver.go"],
    embedsrcs = ["api_tools_manifest.json"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/mcpserver",
    deps = [
        "//enterprise/server/mcp/jsonrpc",
        "//enterprise/server/mcp/mcptools",
        "//server/capabilities_filter",
        "//server/http/interceptors",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/version",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//reflect/protoregistry",
    ],
)

go_test(
    name = "mcpserver_test",
    srcs = ["mcpserver_test.go"],
    embed = [":mcpserver"],
    deps = [
        "//enterprise/server/mcp/jsonrpc",
        "//proto:capability_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/util/authutil",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
