load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "jsonschema",
    srcs = [
        "annotations.go",
        "doc.go",
        "infer.go",
        "json_pointer.go",
        "resolve.go",
        "schema.go",
        "util.go",
        "validate.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/sdk/jsonschema",
    visibility = ["//visibility:public"],
    deps = ["//experimental/mcp/sdk/internal/util"],
)

go_test(
    name = "jsonschema_test",
    srcs = [
        "infer_test.go",
        "json_pointer_test.go",
        "resolve_test.go",
        "schema_test.go",
        "util_test.go",
        "validate_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":jsonschema"],
    deps = [
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_cmp//cmp/cmpopts",
    ],
)
