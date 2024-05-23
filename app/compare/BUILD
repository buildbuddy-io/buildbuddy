load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare_invocations",
    srcs = ["compare_invocations.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/compare:compare_execution_log_files",
        "//app/components/button:checkbox_button",
        "//app/invocation:invocation_model",
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
    name = "compare_execution_log_files",
    srcs = ["compare_execution_log_files.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/digest",
        "//app/components/link",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//proto:build_event_stream_ts_proto",
        "//proto:spawn_ts_proto",
        "@npm//@types/react",
        "@npm//@types/varint",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
        "@npm//varint",
    ],
)
