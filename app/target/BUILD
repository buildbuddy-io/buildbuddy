load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "target",
    srcs = ["target.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
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
    ],
)

ts_library(
    name = "flaky_target_chip",
    srcs = ["flaky_target_chip.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/spinner",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:target_ts_proto",
    ],
)

ts_library(
    name = "action_card",
    srcs = ["action_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/docs:setup_code",
        "//app/service:rpc_service",
        "//app/terminal",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_artifacts_card",
    srcs = ["target_artifacts_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/capabilities",
        "//app/components/digest",
        "//app/service:rpc_service",
        "//app/util:cache",
        "//proto:build_event_stream_ts_proto",
        "//proto:zip_ts_proto",
    ],
)

ts_library(
    name = "target_log_card",
    srcs = ["target_log_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/terminal",
    ],
)

ts_library(
    name = "target_test_cases_card",
    srcs = ["target_test_cases_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/format",
        "//app/target:target_test_suite",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_test_document_card",
    srcs = ["target_test_document_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/service:rpc_service",
        "//app/target:target_log_card",
        "//app/target:target_test_cases_card",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_test_log_card",
    srcs = ["target_test_log_card.tsx"],
    deps = [
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//app/components/link",
        "//app/docs:setup_code",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/terminal",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_flaky_test_card",
    srcs = ["target_flaky_test_card.tsx"],
    deps = [
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//app/components/link",
        "//app/format",
        "//app/router",
        "//app/target:target_test_suite",
        "//app/util:proto",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_test_coverage_card",
    srcs = ["target_test_coverage_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:color",
        "//app/util:lcov",
        "//proto:build_event_stream_ts_proto",
    ],
)

ts_library(
    name = "target_test_suite",
    srcs = ["target_test_suite.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/terminal",
    ],
)

ts_library(
    name = "target_v2",
    srcs = ["target_v2.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/format",
        "//app/invocation:cache_requests_card",
        "//app/invocation:invocation_model",
        "//app/invocation:link_github_repo_modal",
        "//app/invocation:target_util",
        "//app/router",
        "//app/service:rpc_service",
        "//app/target:action_card",
        "//app/target:flaky_target_chip",
        "//app/target:target_artifacts_card",
        "//app/target:target_test_coverage_card",
        "//app/target:target_test_document_card",
        "//app/target:target_test_log_card",
        "//app/util:clipboard",
        "//app/util:proto",
        "//app/util:remote_runner",
        "//proto:build_event_stream_ts_proto",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
    ],
)
