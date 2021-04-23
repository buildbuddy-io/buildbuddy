load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["executors.css"])

ts_library(
    name = "executors",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/format",
        "//app/service",
        "//app/util:errors",
        "//proto:scheduler_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//moment",
        "@npm//react",
        "@npm//rxjs",
    ],
)
