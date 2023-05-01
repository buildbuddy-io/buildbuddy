load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "login",
    srcs = ["login.tsx"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/input",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)
