load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "dense_invocation_overview",
    srcs = ["dense_invocation_overview.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/invocation:invocation_buttons",
        "//app/invocation:invocation_model",
        "//app/router",
    ],
)
