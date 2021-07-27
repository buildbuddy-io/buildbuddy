load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "login",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/alert",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/input",
        "//app/router",
        "//app/service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//protobufjs",
        "@npm//react",
    ],
)
