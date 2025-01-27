load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["snapshots.css"])

ts_library(
    name = "snapshots",
    srcs = ["snapshots.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//app/util:proto",
        "//proto:firecracker_ts_proto",
        "//proto:git_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:runner_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)
