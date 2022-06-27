load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "target",
    srcs = glob(["**/*.tsx"]),
    deps = [
        "//app/alert",
        "//app/auth",
        "//app/components/button",
        "//app/docs",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/terminal",
        "//app/util:clipboard",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
