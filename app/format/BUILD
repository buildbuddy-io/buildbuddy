load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = glob(["*.tsx"]),
    deps = [
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//long",
        "@npm//moment",
    ],
)
