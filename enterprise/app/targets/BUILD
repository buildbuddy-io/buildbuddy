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
        "//app/auth:user",
        "//app/components/select:select",
        "//app/components/spinner:spinner",
        "//app/format:format",
        "//app/router:router",
        "//app/service:rpc_service",
        "//enterprise/app/filter:filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:common",
        "//proto:stats_ts_proto",
        "//proto:stat_filter_ts_proto",
    ],
)
