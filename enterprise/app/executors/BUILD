load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["executors.css"])

ts_library(
    name = "executors",
    srcs = ["executors.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/banner",
        "//app/components/button:link_button",
        "//app/components/link",
        "//app/components/select",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//enterprise/app/executors:executor_card",
        "//proto:api_key_ts_proto",
        "//proto:bazel_config_ts_proto",
        "//proto:capability_ts_proto",
        "//proto:scheduler_ts_proto",
    ],
)

ts_library(
    name = "executor_card",
    srcs = ["executor_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/link",
        "//app/format",
        "//enterprise/app/trends:common",
        "//proto:scheduler_ts_proto",
        "//proto:stat_filter_ts_proto",
    ],
)
