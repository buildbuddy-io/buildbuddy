load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files([
    "code.css",
    "monaco.css",
])

ts_library(
    name = "code",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/service",
        "@npm//@types/diff-match-patch",
        "@npm//@types/react",
        "@npm//diff-match-patch",
        "@npm//monaco-editor",
        "@npm//octokit",
        "@npm//octokit-plugin-create-pull-request",
        "@npm//react",
        "@npm//rxjs",
    ],
)
