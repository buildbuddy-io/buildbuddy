load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "capabilities",
    srcs = ["capabilities.tsx"],
    strict = True,
    deps = [
        "//proto:config_ts_proto",
        "@npm//tslib",
    ],
)
