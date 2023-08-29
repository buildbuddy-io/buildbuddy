load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "invocation",
    srcs = ["invocation.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/favicon",
        "//app/invocation:cache_requests_card",
        "//app/invocation:child_invocations",
        "//app/invocation:invocation_action_card",
        "//app/invocation:invocation_artifacts_card",
        "//app/invocation:invocation_bot_card",
        "//app/invocation:invocation_build_logs_card",
        "//app/invocation:invocation_cache_card",
        "//app/invocation:invocation_details_card",
        "//app/invocation:invocation_error_card",
        "//app/invocation:invocation_execution_card",
        "//app/invocation:invocation_fetch_card",
        "//app/invocation:invocation_filter",
        "//app/invocation:invocation_in_progress",
        "//app/invocation:invocation_logs_model",
        "//app/invocation:invocation_model",
        "//app/invocation:invocation_not_found",
        "//app/invocation:invocation_overview",
        "//app/invocation:invocation_query_graph_card",
        "//app/invocation:invocation_raw_logs_card",
        "//app/invocation:invocation_suggestion_card",
        "//app/invocation:invocation_tabs",
        "//app/invocation:invocation_targets",
        "//app/invocation:invocation_timing_card",
        "//app/invocation:scorecard_card",
        "//app/invocation/dense:dense_invocation_overview",
        "//app/preferences",
        "//app/router",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "//app/target",
        "//app/target:target_v2",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "cache_requests_card",
    srcs = ["cache_requests_card.tsx"],
    deps = [
        "//app/capabilities",
        "//app/components/button",
        "//app/components/digest",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/components/select",
        "//app/components/spinner",
        "//app/components/tooltip",
        "//app/errors:error_service",
        "//app/format",
        "//app/invocation:invocation_execution_util",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//app/util:proto",
        "//proto:cache_ts_proto",
        "//proto:field_mask_ts_proto",
        "//proto:invocation_status_ts_proto",
        "//proto:resource_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "child_invocation_card",
    srcs = ["child_invocation_card.tsx"],
    deps = [
        "//app/components/link",
        "//app/format",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "child_invocations",
    srcs = ["child_invocations.tsx"],
    deps = [
        "//app/invocation:child_invocation_card",
        "//app/invocation:invocation_model",
        "//app/util:proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_action_card",
    srcs = ["invocation_action_card.tsx"],
    deps = [
        "//app/components/digest",
        "//app/components/link",
        "//app/format",
        "//app/invocation:invocation_action_input_node",
        "//app/invocation:invocation_model",
        "//app/preferences",
        "//app/service:rpc_service",
        "//app/terminal",
        "//proto:remote_execution_ts_proto",
        "//proto:resource_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_action_input_node",
    srcs = ["invocation_action_input_node.tsx"],
    deps = [
        "//app/components/digest",
        "//app/format",
        "//proto:remote_execution_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_artifacts_card",
    srcs = ["invocation_artifacts_card.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "//app/invocation:invocation_target_group_card",
        "//app/service:rpc_service",
        "//proto:build_event_stream_ts_proto",
        "//proto:target_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_breakdown_card",
    srcs = ["invocation_breakdown_card.tsx"],
    deps = [
        "//app/format",
        "//app/util:color",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//recharts",
    ],
)

ts_library(
    name = "invocation_build_logs_card",
    srcs = ["invocation_build_logs_card.tsx"],
    deps = [
        "//app/terminal",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_buttons",
    srcs = ["invocation_buttons.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/invocation:invocation_cancel_button",
        "//app/invocation:invocation_compare_button",
        "//app/invocation:invocation_menu",
        "//app/invocation:invocation_model",
        "//app/invocation:invocation_share_button",
        "//app/invocation:suggestion_button",
        "//app/invocation:workflow_rerun_button",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_cache_card",
    srcs = ["invocation_cache_card.tsx"],
    deps = [
        "//app/capabilities",
        "//app/format",
        "//app/invocation:invocation_model",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//recharts",
    ],
)

ts_library(
    name = "invocation_cancel_button",
    srcs = ["invocation_cancel_button.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_compare_button",
    srcs = ["invocation_compare_button.tsx"],
    deps = [
        "//app/capabilities",
        "//app/components/button",
        "//app/components/menu",
        "//app/components/popup",
        "//app/router",
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_details_card",
    srcs = ["invocation_details_card.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/banner",
        "//app/invocation:invocation_model",
        "//app/util:clipboard",
        "//proto:command_line_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//shlex",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_error_card",
    srcs = ["invocation_error_card.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//app/terminal",
        "//proto:failure_details_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_execution_card",
    srcs = ["invocation_execution_card.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/select",
        "//app/invocation:invocation_execution_table",
        "//app/invocation:invocation_execution_util",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//proto:execution_stats_ts_proto",
        "//proto:remote_execution_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_execution_table",
    srcs = ["invocation_execution_table.tsx"],
    deps = [
        "//app/components/digest",
        "//app/components/link",
        "//app/format",
        "//app/invocation:invocation_execution_util",
        "//proto:execution_stats_ts_proto",
        "//proto:remote_execution_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_execution_util",
    srcs = ["invocation_execution_util.tsx"],
    deps = [
        "//proto:execution_stats_ts_proto",
        "//proto:grpc_code_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_fetch_card",
    srcs = ["invocation_fetch_card.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_filter",
    srcs = ["invocation_filter.tsx"],
    deps = [
        "//app/components/filter_input",
        "//app/router",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_in_progress",
    srcs = ["invocation_in_progress.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_logs_model",
    srcs = ["invocation_logs_model.tsx"],
    deps = [
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:eventlog_ts_proto",
        "@npm//rxjs",
    ],
)

ts_library(
    name = "invocation_menu",
    srcs = ["invocation_menu.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_model",
    srcs = ["invocation_model.tsx"],
    deps = [
        "//app/capabilities",
        "//app/favicon",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//proto:api_key_ts_proto",
        "//proto:build_event_stream_ts_proto",
        "//proto:cache_ts_proto",
        "//proto:command_line_ts_proto",
        "//proto:group_ts_proto",
        "//proto:invocation_status_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:suggestion_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_not_found",
    srcs = ["invocation_not_found.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_overview",
    srcs = ["invocation_overview.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/link",
        "//app/format",
        "//app/invocation:invocation_buttons",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/util:git",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_query_graph_card",
    srcs = ["invocation_query_graph_card.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "@npm//@types/dagre-d3",  # keep
        "@npm//@types/react",
        "@npm//dagre-d3-react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_raw_logs_card",
    srcs = ["invocation_raw_logs_card.tsx"],
    deps = [
        "//app/components/banner",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/filter_input",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_share_button",
    srcs = ["invocation_share_button.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/modal",
        "//app/components/select",
        "//app/components/spinner",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "//proto:acl_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_suggestion_card",
    srcs = ["invocation_suggestion_card.tsx"],
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/link",
        "//app/invocation:invocation_model",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_tabs",
    srcs = ["invocation_tabs.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_targets",
    srcs = ["invocation_targets.tsx"],
    deps = [
        "//app/invocation:invocation_model",
        "//app/invocation:invocation_target_group_card",
        "//app/invocation:invocation_targets_card",
        "//app/service:rpc_service",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "invocation_targets_card",
    srcs = ["invocation_targets_card.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/link",
        "//app/invocation:invocation_model",
        "//app/util:clipboard",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_timing_card",
    srcs = ["invocation_timing_card.tsx"],
    deps = [
        "//app/components/button",
        "//app/docs:setup_code",
        "//app/errors:error_service",
        "//app/flame_chart",
        "//app/flame_chart:profile_model",
        "//app/format",
        "//app/invocation:invocation_breakdown_card",
        "//app/invocation:invocation_model",
        "//app/invocation:invocation_suggestion_card",
        "//app/service:rpc_service",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "scorecard_card",
    srcs = ["scorecard_card.tsx"],
    deps = [
        "//app/components/link",
        "//app/invocation:invocation_model",
        "//app/router",
        "//proto:cache_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "workflow_rerun_button",
    srcs = ["workflow_rerun_button.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/button:button_group",
        "//app/components/dialog",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:workflow_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_bot_card",
    srcs = ["invocation_bot_card.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "suggestion_button",
    srcs = ["suggestion_button.tsx"],
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/button:button_group",
        "//app/components/dialog",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "invocation_target_group_card",
    srcs = ["invocation_target_group_card.tsx"],
    deps = [
        "//app/components/link",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/invocation:target_util",
        "//app/service:rpc_service",
        "//app/util:clipboard",
        "//proto:build_event_stream_ts_proto",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "target_util",
    srcs = ["target_util.tsx"],
    deps = [
        "//app/util:proto",
        "//proto/api/v1:common_ts_proto",
    ],
)
