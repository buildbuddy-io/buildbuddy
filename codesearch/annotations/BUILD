load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "annotations",
    srcs = [
        "annotations.go",
        "golang.go",
        "java.go",
        "python.go",
        "rust.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/annotations",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/indexprofile",
        "@com_github_smacker_go_tree_sitter//:go-tree-sitter",
        "@com_github_smacker_go_tree_sitter//golang",
        "@com_github_smacker_go_tree_sitter//java",
        "@com_github_smacker_go_tree_sitter//python",
        "@com_github_smacker_go_tree_sitter//rust",
        "@org_golang_x_mod//modfile",
    ],
)

go_test(
    name = "annotations_test",
    srcs = [
        "annotations_test.go",
        "python_test.go",
        "rust_test.go",
    ],
    deps = [
        ":annotations",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
