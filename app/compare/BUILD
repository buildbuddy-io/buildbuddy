load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare_invocations",
    srcs = ["compare_invocations.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/compare:compare_execution_log_files",
        "//app/components/button:checkbox_button",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//app/util:diff",
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

ts_library(
    name = "compare_actions",
    srcs = ["compare_actions.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/compare:tree_differ",
        "//app/components/button:checkbox_button",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//app/util:cache",
        "//app/util:diff",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "//proto:remote_execution_ts_proto",
    ],
)

ts_library(
    name = "tree_utils",
    srcs = ["tree_utils.ts"],
    deps = [
        "//app/invocation:invocation_action_tree_node",
        "//proto:remote_execution_ts_proto",
    ],
)

ts_library(
    name = "tree_differ",
    srcs = ["tree_differ.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/compare:diff_tree_node",
        "//app/compare:tree_utils",
        "//app/invocation:invocation_action_tree_node",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//proto:remote_execution_ts_proto",
    ],
)

ts_library(
    name = "diff_tree_node",
    srcs = ["diff_tree_node.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/compare:tree_utils",
        "//app/components/digest",
        "//app/format",
        "//app/invocation:invocation_action_tree_node",
        "//app/invocation:invocation_model",
        "//proto:remote_execution_ts_proto",
    ],
)
