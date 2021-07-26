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
        "//app/util:proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-date-range",
        "@npm//date-fns",
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
        "//:node_modules/react-date-range/dist/styles.css",
        "//:node_modules/react-date-range/dist/theme/default.css",
    ],
)
