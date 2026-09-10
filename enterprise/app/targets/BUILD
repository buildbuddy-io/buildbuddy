load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "single_target.css",
    "targets.css",
])

ts_library(
    name = "single_target",
    srcs = ["single_target.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/components/breadcrumbs",
        "//app/components/button",
        "//app/components/chart:donut_chart",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:color",
        "//app/util:diff",
        "//app/util:proto",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/targets:single_action",
        "//enterprise/app/targets:target_chart",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:trends_chart",
        "//proto:execution_stats_ts_proto",
        "//proto:stats_ts_proto",
    ],
)

ts_library(
    name = "targets",
    srcs = ["targets.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:trends_chart",
        "//proto:stat_filter_ts_proto",
        "//proto:stats_ts_proto",
    ],
)

ts_library(
    name = "target_chart",
    srcs = ["target_chart.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/format",
        "//app/router",
        "//enterprise/app/trends:trends_chart",
        "//proto:execution_stats_ts_proto",
    ],
)

ts_library(
    name = "single_action",
    srcs = ["single_action.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//app/components/button",
        "//app/format",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:trends_chart",
        "//proto:execution_stats_ts_proto",
        "//proto:stats_ts_proto",
    ],
)
