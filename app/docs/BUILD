load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "docs",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/protobufjs",
        "//:node_modules/react",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/router",
        "//app/service",
        "//proto:bazel_config_ts_proto",
    ],
)
