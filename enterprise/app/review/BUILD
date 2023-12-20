load("//rules/typescript:index.bzl", "ts_library")

exports_files(["review.css"])

ts_library(
    name = "review",
    srcs = ["review.tsx"],
    deps = [
        "//app/auth:user",
        "//app/format",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
        "@npm//tslib",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
