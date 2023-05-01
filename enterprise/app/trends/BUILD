load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["trends.css"])

ts_library(
    name = "trends",
    srcs = ["trends.tsx"],
    strict = True,
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:cache_chart",
        "//enterprise/app/trends:drilldown_page",
        "//enterprise/app/trends:percentile_chart",
        "//enterprise/app/trends:summary_card",
        "//enterprise/app/trends:trends_chart",
        "//proto:stats_ts_proto",
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "cache_chart",
    srcs = ["cache_chart.tsx"],
    strict = True,
    deps = [
        "//app/format",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "drilldown_page",
    srcs = ["drilldown_page.tsx"],
    strict = True,
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/invocation:invocation_execution_table",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/history:history_invocation_card",
        "//enterprise/app/trends:heatmap",
        "//proto:execution_stats_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:stat_filter_ts_proto",
        "//proto:stats_ts_proto",
        "//proto:timestamp_ts_proto",
        "//:node_modules/@types/long",
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "heatmap",
    srcs = ["heatmap.tsx"],
    strict = True,
    deps = [
        "//app/components/tooltip",
        "//proto:stats_ts_proto",
        "//:node_modules/@types/d3-scale",
        "//:node_modules/@types/long",
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/d3-scale",
        "//:node_modules/long",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/react-resize-detector",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "percentile_chart",
    srcs = ["percentile_chart.tsx"],
    strict = True,
    deps = [
        "//app/format",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "summary_card",
    srcs = ["summary_card.tsx"],
    strict = True,
    deps = [
        "//app/format",
        "//enterprise/app/filter:filter_util",
        "//proto:stats_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "trends_chart",
    srcs = ["trends_chart.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
    ],
)
