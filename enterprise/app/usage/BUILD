load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["usage.css"])

ts_library(
    name = "usage",
    srcs = ["usage.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/trends:trends_chart",
        "//proto:usage_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
    ],
)
