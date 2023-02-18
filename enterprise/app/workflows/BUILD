load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["workflows.css"])

ts_library(
    name = "workflows",
    srcs = ["workflows.tsx"],
    strict = True,
    deps = [
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/dialog",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//app/util:errors",
        "//enterprise/app/workflows:create_workflow",
        "//enterprise/app/workflows:github_import",
        "//enterprise/app/workflows:zero_state",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "create_workflow",
    srcs = ["create_workflow.tsx"],
    strict = True,
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
    strict = True,
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
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
    ],
)
