load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["workflows.css"])

ts_library(
    name = "workflows",
    srcs = ["workflows.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/checkbox",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/menu",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/components/tooltip",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//app/util:git",
        "//enterprise/app/workflows:action_list",
        "//enterprise/app/workflows:create_workflow",
        "//enterprise/app/workflows:github_app_import",
        "//enterprise/app/workflows:github_import",
        "//enterprise/app/workflows:zero_state",
        "//proto:github_ts_proto",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "action_list",
    srcs = ["action_list.tsx"],
    deps = [
        "//app/components/link",
        "//app/components/tooltip",
        "//app/format",
        "//app/router",
        "//app/util:proto",
        "//proto:invocation_status_ts_proto",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "create_workflow",
    srcs = ["create_workflow.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "github_import",
    srcs = ["github_import.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/link",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "zero_state",
    srcs = ["zero_state.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "github_app_import",
    srcs = ["github_app_import.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:user",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:git",
        "//proto:github_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
