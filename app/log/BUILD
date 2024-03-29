load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "log",
    srcs = ["log.tsx"],
    deps = [
        "//app/format",
        "//app/invocation:invocation_card",
        "//app/service:rpc_service",
        "//app/terminal:ansi",
        "//app/terminal:row",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

exports_files(glob(["*.css"]))
