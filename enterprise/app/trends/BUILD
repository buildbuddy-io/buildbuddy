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
        "//app/components/button:checkbox_button",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/trends:cache_chart",
        "//enterprise/app/trends:drilldown_page",
        "//enterprise/app/trends:percentile_chart",
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
    name = "cache_chart",
    srcs = ["cache_chart.tsx"],
    strict = True,
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
    strict = True,
    deps = [
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/components/spinner",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/history:history_invocation_card",
        "//enterprise/app/trends:heatmap",
        "//proto:invocation_ts_proto",
        "//proto:stat_filter_ts_proto",
        "//proto:stats_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
        "@npm//recharts",
        "@npm//tslib",
    ],
)

ts_library(
    name = "heatmap",
    srcs = ["heatmap.tsx"],
    strict = True,
    deps = [
        "//app/components/tooltip",
        "//proto:stats_ts_proto",
        "@npm//@types/d3-scale",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//d3-scale",
        "@npm//moment",
        "@npm//react",
        "@npm//react-resize-detector",
        "@npm//tslib",
    ],
)

ts_library(
    name = "percentile_chart",
    srcs = ["percentile_chart.tsx"],
    strict = True,
    deps = [
        "//app/format",
        "@npm//@types/react",
        "@npm//react",
        "@npm//recharts",
        "@npm//tslib",
    ],
)

ts_library(
    name = "trends_chart",
    srcs = ["trends_chart.tsx"],
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//recharts",
    ],
)
