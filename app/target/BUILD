load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "target",
    srcs = ["target.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/components/button:link_button",
        "//app/format",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/target:action_card",
        "//app/target:target_artifacts_card",
        "//app/target:target_test_coverage_card",
        "//app/target:target_test_document_card",
        "//app/target:target_test_log_card",
        "//app/util:clipboard",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "action_card",
    srcs = ["action_card.tsx"],
    deps = [
        "//app/docs:setup_code",
        "//app/service:rpc_service",
        "//app/terminal",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_artifacts_card",
    srcs = ["target_artifacts_card.tsx"],
    deps = [
        "//app/capabilities",
        "//app/components/digest",
        "//app/service:rpc_service",
        "//app/util:cache",
        "//proto:build_event_stream_ts_proto",
        "//proto:zip_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_log_card",
    srcs = ["target_log_card.tsx"],
    deps = [
        "//app/terminal",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_test_cases_card",
    srcs = ["target_test_cases_card.tsx"],
    deps = [
        "//app/format",
        "//app/terminal",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_test_document_card",
    srcs = ["target_test_document_card.tsx"],
    deps = [
        "//app/service:rpc_service",
        "//app/target:target_log_card",
        "//app/target:target_test_cases_card",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_test_log_card",
    srcs = ["target_test_log_card.tsx"],
    deps = [
        "//app/docs:setup_code",
        "//app/format",
        "//app/service:rpc_service",
        "//app/terminal",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_test_coverage_card",
    srcs = ["target_test_coverage_card.tsx"],
    deps = [
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:color",
        "//app/util:lcov",
        "//proto:build_event_stream_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "target_v2",
    srcs = ["target_v2.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button:link_button",
        "//app/errors:error_service",
        "//app/format",
        "//app/invocation:cache_requests_card",
        "//app/invocation:invocation_model",
        "//app/invocation:target_util",
        "//app/router",
        "//app/service:rpc_service",
        "//app/target:action_card",
        "//app/target:target_artifacts_card",
        "//app/target:target_test_coverage_card",
        "//app/target:target_test_document_card",
        "//app/target:target_test_log_card",
        "//app/util:clipboard",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
