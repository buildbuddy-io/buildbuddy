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
        "//app/components/button",
        "//app/components/menu",
        "//app/components/popup",
        "//app/format",
        "//app/service",
        "//proto:invocation_ts_proto",
        "//proto:runner_ts_proto",
        "@npm//@types/diff",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//monaco-editor",
        "@npm//octokit",
        "@npm//octokit-plugin-create-pull-request",
        "@npm//react",
        "@npm//rxjs",
    ],
)
