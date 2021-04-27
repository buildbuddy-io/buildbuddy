load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["trends.css"])

ts_library(
    name = "trends",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/service",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
        "@npm//recharts",
        "@npm//rxjs",
    ],
)
