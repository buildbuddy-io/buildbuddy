load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["usage.css"])

ts_library(
    name = "usage",
    srcs = ["usage.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/trends:trends_chart",
        "//enterprise/app/usage:usage_alerts",
        "//proto:usage_ts_proto",
    ],
)

ts_library(
    name = "usage_alerts_model",
    srcs = ["usage_alerts_model.ts"],
    deps = [
        "//proto:usage_ts_proto",
    ],
)

ts_library(
    name = "usage_alerts",
    srcs = ["usage_alerts.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/components/tooltip:help_tooltip",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//enterprise/app/usage:usage_alerts_model",
        "//proto:usage_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "usage_alerts_model_test",
    srcs = ["usage_alerts_model_test.ts"],
    deps = [
        ":usage_alerts_model",
        "//:node_modules/tslib",
        "//proto:usage_ts_proto",
    ],
)
