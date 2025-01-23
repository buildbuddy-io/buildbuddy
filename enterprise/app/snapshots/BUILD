load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["snapshots.css"])

ts_library(
    name = "snapshots",
    srcs = ["snapshots.tsx"],
    deps = [
        "//app/auth:user",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:firecracker_ts_proto",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
