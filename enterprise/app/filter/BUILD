load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "filter",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/popup",
        "//app/components/radio",
        "//app/components/slider",
        "//app/format",
        "//app/router",
        "//app/util:proto",
        "//proto:invocation_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-date-range",
        "@npm//date-fns",
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
