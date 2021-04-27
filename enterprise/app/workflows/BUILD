load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(["workflows.css"])

ts_library(
    name = "workflows",
    srcs = glob(["*.tsx"]),
    deps = [
        "//app/auth",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/input",
        "//app/components/menu",
        "//app/components/modal",
        "//app/components/popup",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//app/util:clipboard",
        "//app/util:errors",
        "//proto:workflow_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
    ],
)
