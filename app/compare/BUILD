load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare",
    srcs = glob(["**/*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "@npm//@types/diff",
        "@npm//@types/react",
        "@npm//diff",
        "@npm//react",
        "@npm//tslib",
    ],
)
