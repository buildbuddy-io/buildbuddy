load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/components/select",
        "//app/format",
        "//app/router",
        "//app/service",
        "//proto:invocation_ts_proto",
        "//proto:target_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//moment",
        "@npm//react",
        "@npm//recharts",
        "@npm//rxjs",
    ],
)
