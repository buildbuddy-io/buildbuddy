load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "router",
    srcs = ["router.tsx"],
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/format",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "@npm//tslib",
    ],
)
