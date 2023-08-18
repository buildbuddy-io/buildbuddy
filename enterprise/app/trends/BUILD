load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["trends.css"])

ts_library(
    name = "trends",
    srcs = ["trends.tsx"],
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
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "common",
    srcs = ["common.ts"],
)

ts_library(
    name = "simple_trends_tab",
    srcs = ["simple_trends_tab.tsx"],
    deps = [
        "//app/capabilities",
        "//app/format",
        "//app/router",
        "//app/util:async",
        "//enterprise/app/trends:cache_chart",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:percentile_chart",
        "//enterprise/app/trends:summary_card",
        "//enterprise/app/trends:trends_chart",
        "//enterprise/app/trends:trends_model",
        "//enterprise/app/trends:trends_requests",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "new_trends",
    srcs = ["new_trends.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//enterprise/app/filter",
        "//enterprise/app/trends:common",
        "//enterprise/app/trends:drilldown_page",
        "//enterprise/app/trends:simple_trends_tab",
        "//enterprise/app/trends:trends_model",
        "//enterprise/app/trends:trends_requests",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "trends_model",
    srcs = ["trends_model.ts"],
    deps = [
        "//app/util:proto",
        "//proto:stats_ts_proto",
        "@npm//@types/moment",
        "@npm//moment",
    ],
)

ts_library(
    name = "trends_requests",
    srcs = ["trends_requests.ts"],
    deps = [
        "//app/capabilities",
        "//app/service:rpc_service",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:trends_model",
        "//proto:stats_ts_proto",
        "@npm//@types/moment",
        "@npm//moment",
        "@npm//tslib",
    ],
)

ts_library(
    name = "cache_chart",
    srcs = ["cache_chart.tsx"],
    deps = [
        "//app/format",
        "@npm//@types/react",
        "@npm//react",
        "@npm//recharts",
        "@npm//tslib",
    ],
)

ts_library(
    name = "drilldown_page",
    srcs = ["drilldown_page.tsx"],
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
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//recharts",
        "@npm//tslib",
    ],
)

ts_library(
    name = "heatmap",
    srcs = ["heatmap.tsx"],
    deps = [
        "//app/components/tooltip",
        "//proto:stats_ts_proto",
        "@npm//@types/d3-scale",
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//d3-scale",
        "@npm//long",
        "@npm//moment",
        "@npm//react",
        "@npm//react-resize-detector",
        "@npm//tslib",
    ],
)

ts_library(
    name = "percentile_chart",
    srcs = ["percentile_chart.tsx"],
    deps = [
        "//app/format",
        "@npm//@types/react",
        "@npm//react",
        "@npm//recharts",
        "@npm//tslib",
    ],
)

ts_library(
    name = "summary_card",
    srcs = ["summary_card.tsx"],
    deps = [
        "//app/format",
        "//enterprise/app/filter:filter_util",
        "//proto:stats_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "trends_chart",
    srcs = ["trends_chart.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//recharts",
    ],
)
