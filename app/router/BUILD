load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "router_params",
    srcs = ["router_params.ts"],
)

ts_library(
    name = "router",
    srcs = ["router.tsx"],
    deps = [
        ":router_params",
        "//app/auth:user",
        "//app/capabilities",
        "//app/format",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "@npm//tslib",
    ],
)
