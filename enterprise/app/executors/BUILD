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
        "//app/components/select",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//enterprise/app/executors:executor_card",
        "//proto:api_key_ts_proto",
        "//proto:bazel_config_ts_proto",
        "//proto:scheduler_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "executor_card",
    srcs = ["executor_card.tsx"],
    deps = [
        "//app/format",
        "//proto:scheduler_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)
