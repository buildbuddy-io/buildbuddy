load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "code.css",
    "monaco.css",
])

ts_library(
    name = "code",
    srcs = ["code.tsx"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/service:rpc_service",
        "//app/util:lcov",
        "//enterprise/app/code:code_build_button",
        "//enterprise/app/code:code_empty",
        "//enterprise/app/code:code_pull_request",
        "//enterprise/app/code:code_sidebar_node",
        "//proto:runner_ts_proto",
        "@npm//@octokit/rest",
        "@npm//@types/diff",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//lucide-react",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "code_build_button",
    srcs = ["code_build_button.tsx"],
    strict = True,
    deps = [
        "//app/components/button",
        "//app/components/button:button_group",
        "//app/components/menu",
        "//app/components/popup",
        "//app/components/spinner",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "code_empty",
    srcs = ["code_empty.tsx"],
    strict = True,
    deps = [
        "//app/format",
        "//app/service:rpc_service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "code_pull_request",
    srcs = ["code_pull_request.ts"],
    strict = True,
    deps = [
        "@npm//@octokit/rest",
        "@npm//tslib",
    ],
)

ts_library(
    name = "code_sidebar_node",
    srcs = ["code_sidebar_node.tsx"],
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
