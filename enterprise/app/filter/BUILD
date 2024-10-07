load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "filter",
    srcs = ["filter.tsx"],
    deps = [
        "//app/capabilities",
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/popup",
        "//app/components/radio",
        "//app/components/slider",
        "//app/format",
        "//app/router",
        "//app/router:router_params",
        "//enterprise/app/filter:date_picker_button",
        "//enterprise/app/filter:filter_util",
        "//proto:invocation_status_ts_proto",
        "//proto:stat_filter_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
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
    name = "date_picker_button",
    srcs = ["date_picker_button.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/popup",
        "//app/format",
        "//app/router",
        "//app/router:router_params",
        "//enterprise/app/filter:filter_util",
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

ts_library(
    name = "filter_util",
    srcs = ["filter_util.tsx"],
    deps = [
        "//app/capabilities",
        "//app/format",
        "//app/router:router_params",
        "//app/util:proto",
        "//proto:duration_ts_proto",
        "//proto:invocation_status_ts_proto",
        "//proto:stat_filter_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//long",
        "@npm//moment",
        "@npm//tslib",
    ],
)
