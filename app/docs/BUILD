load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "docs",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//proto:bazel_config_ts_proto",
        "@npm//@types/react",
        "@npm//protobufjs",
        "@npm//react",
    ],
)
