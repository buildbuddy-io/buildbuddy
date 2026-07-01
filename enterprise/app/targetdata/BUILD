load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["targetdata.css"])

ts_library(
    name = "targetdata",
    srcs = ["targetdata.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:trends_chart",
        "//proto:execution_stats_ts_proto",
    ],
)
