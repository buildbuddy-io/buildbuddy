load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "router",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/format",
        "@npm//tslib",
    ],
)
