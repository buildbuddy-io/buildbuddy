load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["usage.css"])

ts_library(
    name = "usage",
    srcs = ["usage.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/trends:trends_chart",
        "//proto:usage_ts_proto",
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//tslib",
    ],
)
