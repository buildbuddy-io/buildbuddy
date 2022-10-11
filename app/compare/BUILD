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
        "//:node_modules/@types/diff-match-patch",
        "//:node_modules/@types/react",
        "//:node_modules/diff-match-patch",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth",
        "//app/components/button",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:build_event_stream_ts_proto",
        "//proto:command_line_ts_proto",
        "//proto:invocation_ts_proto",
    ],
)
