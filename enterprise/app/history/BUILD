load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "history",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/docs",
        "//app/format",
        "//app/router",
        "//app/service",
        "//enterprise/app/filter",
        "//enterprise/app/org",
        "//proto:invocation_ts_proto",
        "//proto:user_ts_proto",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//protobufjs",
        "@npm//react",
        "@npm//rxjs",
    ],
)
