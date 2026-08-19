load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "pie_chart",
    srcs = ["pie_chart.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//app/format",
        "//app/util:color",
    ],
)
