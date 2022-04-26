load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare",
    srcs = glob([
        "**/*.tsx",
        "**/*.ts",
    ]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:build_event_stream_ts_proto",
        "//proto:command_line_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/diff-match-patch",
        "@npm//@types/react",
        "@npm//diff-match-patch",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)
