load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "filter",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/components/button",
        "//app/components/popup",
        "//app/format",
        "//app/router",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-date-range",
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
