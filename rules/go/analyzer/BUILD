load("@io_bazel_rules_go//go:def.bzl", "go_library")
load(":def.bzl", "ANALYZERS")

exports_files([
    "def.bzl",
])

# gazelle:exclude analyzer.go

[
    go_library(
        name = analyzer,
        srcs = ["analyzer.go"],
        importpath = "github.com/buildbuddy-io/buildbuddy/rules/go/analyzer/" + analyzer,
        visibility = ["//visibility:public"],
        x_defs = {"name": analyzer},
        deps = [
            "@org_golang_x_tools//go/analysis",
            "@org_golang_x_tools//go/analysis/passes/modernize",
        ],
    )
    for analyzer in ANALYZERS
]
