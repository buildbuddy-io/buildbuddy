load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "router_params",
    srcs = ["router_params.ts"],
    deps = ["//:node_modules/tslib"],
)

ts_library(
    name = "router",
    srcs = ["router.tsx"],
    deps = [
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/capabilities",
        "//app/format",
        "//app/router:router_params",
        "//app/service:rpc_service",
        "//app/shortcuts",
    ],
)
