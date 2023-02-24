load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare_invocations",
    srcs = ["compare_invocations.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/compare:diff_chunk",
        "//app/compare:diff_preprocessing",
        "//app/components/button",
        "//app/components/button:checkbox_button",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "@npm//@types/diff-match-patch",
        "@npm//@types/react",
        "@npm//diff-match-patch",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "diff_chunk",
    srcs = ["diff_chunk.tsx"],
    deps = [
        "//app/components/button",
        "@npm//@types/diff-match-patch",
        "@npm//@types/react",
        "@npm//diff-match-patch",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "diff_preprocessing",
    srcs = ["diff_preprocessing.tsx"],
    deps = [
        "//proto:build_event_stream_ts_proto",
        "//proto:command_line_ts_proto",
        "//proto:invocation_ts_proto",
    ],
)
