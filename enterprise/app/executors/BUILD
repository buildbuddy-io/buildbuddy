load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["executors.css"])

ts_library(
    name = "executors",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/select",
        "//app/format",
        "//app/router",
        "//app/service",
        "//app/util:errors",
        "//proto:api_key_ts_proto",
        "//proto:scheduler_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//moment",
        "@npm//react",
        "@npm//rxjs",
    ],
)
