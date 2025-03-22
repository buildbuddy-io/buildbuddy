load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare_invocations",
    srcs = ["compare_invocations.tsx"],
    deps = [
        "//:node_modules/@types/diff-match-patch",
        "//:node_modules/@types/react",
        "//:node_modules/diff-match-patch",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/compare:compare_execution_log_files",
        "//app/components/button:checkbox_button",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
    ],
)

ts_library(
    name = "compare_execution_log_files",
    srcs = ["compare_execution_log_files.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/varint",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//:node_modules/varint",
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
    ],
)
