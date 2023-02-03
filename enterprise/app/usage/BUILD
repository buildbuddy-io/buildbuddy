load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["usage.css"])

ts_library(
    name = "usage",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//app/auth",
        "//app/components/select",
        "//app/errors",
        "//app/format",
        "//app/service",
        "//proto:usage_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)
