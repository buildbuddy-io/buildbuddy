load("//rules/typescript:index.bzl", "ts_library")

exports_files(["auditlogs.css"])

ts_library(
    name = "auditlogs",
    srcs = ["auditlogs.tsx"],
    deps = [
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//proto:auditlog_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
