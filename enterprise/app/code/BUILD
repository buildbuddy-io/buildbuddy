load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "code.css",
    "monaco.css",
])

ts_library(
    name = "code",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    deps = [
        "//app/alert",
        "//app/auth",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/format",
        "//app/service",
        "//proto:invocation_ts_proto",
        "//proto:runner_ts_proto",
        "@npm//@types/diff",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//lucide-react",
        "@npm//monaco-editor",
        "@npm//octokit",
        "@npm//react",
        "@npm//rxjs",
    ],
)
