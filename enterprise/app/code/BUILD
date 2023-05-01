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
        "//:node_modules/@octokit/rest",
        "//:node_modules/@types/diff",
        "//:node_modules/@types/react",
        "//:node_modules/diff",
        "//:node_modules/lucide-react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
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
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
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
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "code_pull_request",
    srcs = ["code_pull_request.ts"],
    strict = True,
    deps = [
        "//:node_modules/@octokit/rest",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "code_sidebar_node",
    srcs = ["code_sidebar_node.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)
