load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["usage.css"])

ts_library(
    name = "usage",
    srcs = ["usage.tsx"],
    deps = [
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/trends:trends_chart",
        "//proto:usage_ts_proto",
    ],
)
