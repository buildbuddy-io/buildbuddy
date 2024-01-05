load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["review.css"])

ts_library(
    name = "review",
    srcs = ["review.tsx"],
    deps = [
        "//app/auth:user",
        "//app/router",
        "//enterprise/app/review:review_list",
        "//enterprise/app/review:view_pull_request",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "review_list",
    srcs = ["review_list.tsx"],
    deps = [
        "//app/auth:user",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:react",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
    ],
)

ts_library(
    name = "view_pull_request",
    srcs = ["view_pull_request.tsx"],
    deps = [
        "//app/auth:user",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)
