load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "code.css",
    "monaco.css",
])

ts_library(
    name = "code",
    srcs = ["code.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/search_bar",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/icons:github",
        "//app/picker:picker_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:lcov",
        "//enterprise/app/code:code_build_button",
        "//enterprise/app/code:code_empty",
        "//enterprise/app/code:code_pull_request",
        "//enterprise/app/code:code_sidebar_node",
        "//enterprise/app/monaco",
        "//enterprise/app/org_picker",
        "//enterprise/app/sidekick/bazelrc",
        "//enterprise/app/sidekick/bazelversion",
        "//enterprise/app/sidekick/buildfile",
        "//enterprise/app/sidekick/module",
        "//proto:git_ts_proto",
        "//proto:github_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:runner_ts_proto",
        "//proto:search_ts_proto",
        "@npm//@types/diff",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//long",
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
    deps = [
        "//app/format",
        "//app/service:rpc_service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "code_pull_request",
    srcs = ["code_pull_request.ts"],
    deps = [
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

ts_library(
    name = "code_sidebar_node",
    srcs = ["code_sidebar_node.tsx"],
    deps = [
        "//proto:github_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "code_sidebar_node_v2",
    srcs = ["code_sidebar_node_v2.tsx"],
    deps = [
        "//proto:workspace_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "code_v2",
    srcs = ["code_v2.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/search_bar",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/icons:github",
        "//app/picker:picker_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:lcov",
        "//enterprise/app/code:code_build_button",
        "//enterprise/app/code:code_empty",
        "//enterprise/app/code:code_pull_request",
        "//enterprise/app/code:code_sidebar_node_v2",
        "//enterprise/app/monaco",
        "//enterprise/app/org_picker",
        "//enterprise/app/sidekick/bazelrc",
        "//enterprise/app/sidekick/bazelversion",
        "//enterprise/app/sidekick/buildfile",
        "//enterprise/app/sidekick/module",
        "//proto:git_ts_proto",
        "//proto:github_ts_proto",
        "//proto:kythe_xref_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:runner_ts_proto",
        "//proto:search_ts_proto",
        "//proto:workspace_ts_proto",
        "@npm//@types/diff",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)
