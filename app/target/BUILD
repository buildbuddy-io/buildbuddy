load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "target",
    srcs = glob(["**/*.tsx"]),
    deps = [
        "//app/auth",
        "//app/docs",
        "//app/format",
        "//app/router",
        "//app/terminal",
        "//proto:build_event_stream_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
