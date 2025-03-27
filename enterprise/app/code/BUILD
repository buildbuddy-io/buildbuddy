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
        "//:node_modules/@types/diff",
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/diff",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
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
    ],
)

ts_library(
    name = "code_build_button",
    srcs = ["code_build_button.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/button:button_group",
        "//app/components/menu",
        "//app/components/popup",
        "//app/components/spinner",
    ],
)

ts_library(
    name = "code_empty",
    srcs = ["code_empty.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/format",
        "//app/service:rpc_service",
        "//proto:invocation_ts_proto",
    ],
)

ts_library(
    name = "code_pull_request",
    srcs = ["code_pull_request.ts"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//:node_modules/tslib",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "code_sidebar_node",
    srcs = ["code_sidebar_node.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "code_sidebar_node_v2",
    srcs = ["code_sidebar_node_v2.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//proto:workspace_ts_proto",
    ],
)

ts_library(
    name = "code_v2",
    srcs = ["code_v2.tsx"],
    deps = [
        "//:node_modules/@types/diff",
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/diff",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
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
    ],
)
