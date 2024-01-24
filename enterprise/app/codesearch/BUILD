load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["codesearch.css"])


ts_library(
    name = "codesearch",
    srcs = ["codesearch.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/service:rpc_service",
        "//proto:search_ts_proto",
        "@npm//react",
        "@npm//tslib",
    ],
)

