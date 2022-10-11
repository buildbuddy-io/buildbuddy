load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "target",
    srcs = glob(["**/*.tsx"]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
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
        "//proto:zip_ts_proto",
    ],
)
