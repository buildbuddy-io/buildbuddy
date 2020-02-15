load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "home",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//protobufjs",
        "@npm//react",
    ],
)
