load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "annotations",
    srcs = [
        "golang.go",
        "annotations.go",
        "java.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/annotations",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/indexprofile",
        "@com_github_smacker_go_tree_sitter//:go-tree-sitter",
        "@com_github_smacker_go_tree_sitter//golang",
        "@com_github_smacker_go_tree_sitter//java",
        "@org_golang_x_mod//modfile",
    ],
)

go_test(
    name = "annotations_test",
    srcs = ["annotations_test.go"],
    deps = [
        ":annotations",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
