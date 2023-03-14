load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "filter",
    srcs = ["filter.tsx"],
    strict = True,
    deps = [
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/popup",
        "//app/components/radio",
        "//app/components/slider",
        "//app/format",
        "//app/router",
        "//enterprise/app/filter:filter_util",
        "//proto:invocation_status_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-date-range",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//react-date-range",
        "@npm//tslib",
    ],
)

filegroup(
    name = "css",
    srcs = [
        ":filter.css",
        ":react_date_range_base.css",
        ":react_date_range_default_theme.css",
    ],
)

ts_library(
    name = "filter_util",
    srcs = ["filter_util.tsx"],
    deps = [
        "//app/router",
        "//app/util:proto",
        "//proto:duration_ts_proto",
        "//proto:invocation_status_ts_proto",
        "//proto:stat_filter_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/moment",
        "@npm//moment",
        "@npm//tslib",
    ],
)
