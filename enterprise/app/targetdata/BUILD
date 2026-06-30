load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["targetdata.css"])

ts_library(
    name = "targetdata",
    srcs = ["targetdata.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth:user",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//enterprise/app/filter:filter_util",
        "//proto:execution_stats_ts_proto",
    ],
)
