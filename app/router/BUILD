load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "router",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/capabilities",
        "//app/format",
        "@npm//tslib",
    ],
)
