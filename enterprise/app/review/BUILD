load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["review.css"])

ts_library(
    name = "review",
    srcs = ["review.tsx"],
    deps = [
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
        "//app/components/link",
        "//app/format",
        "//app/router",
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
    name = "review_thread",
    srcs = ["review_thread.tsx"],
    deps = [
        "//proto:github_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//moment",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "view_pull_request",
    srcs = ["view_pull_request.tsx"],
    deps = [
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//enterprise/app/review:review_thread",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)
