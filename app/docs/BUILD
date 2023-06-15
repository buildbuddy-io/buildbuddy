load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "setup",
    srcs = ["setup.tsx"],
    deps = [
        "//app/capabilities",
        "//app/docs:setup_code",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:bazel_config_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "setup_code",
    srcs = ["setup_code.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/button:link_button",
        "//app/components/select",
        "//app/service:rpc_service",
        "//proto:bazel_config_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
