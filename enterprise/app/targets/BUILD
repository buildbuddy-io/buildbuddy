load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["targets.css"])

ts_library(
    name = "targets",
    srcs = ["targets.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:trends_chart",
        "//proto:stat_filter_ts_proto",
        "//proto:stats_ts_proto",
    ],
)
