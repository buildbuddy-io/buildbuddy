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
        "//:node_modules/@octokit/rest",
        "//:node_modules/@types/diff",
        "//:node_modules/@types/react",
        "//:node_modules/diff",
        "//:node_modules/lucide-react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/rxjs",
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
    ],
)
