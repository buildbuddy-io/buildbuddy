load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["trends.css"])

ts_library(
    name = "trends",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/tooltip",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:proto",
        "//enterprise/app/filter",
        "//enterprise/app/history",
        "//proto:invocation_ts_proto",
        "//proto:stats_ts_proto",
        "@npm//@types/d3-scale",
        "@npm//@types/react",
        "@npm//d3-scale",
        "@npm//moment",
        "@npm//react",
        "@npm//react-resize-detector",
        "@npm//recharts",
        "@npm//rxjs",
    ],
)
