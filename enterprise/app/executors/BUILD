load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["executors.css"])

ts_library(
    name = "executors",
    srcs = ["executors.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/link",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//app/util:proto",
        "//enterprise/app/executors:executor_card",
        "//proto:api_key_ts_proto",
        "//proto:bazel_config_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:scheduler_queue_ts_proto",
        "//proto:scheduler_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "executor_card",
    srcs = ["executor_card.tsx"],
    deps = [
        "//app/format",
        "//proto:scheduler_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
