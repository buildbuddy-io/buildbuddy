load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["review.css"])

ts_library(
    name = "review_controller",
    srcs = ["review_controller.ts"],
    deps = [
        "//enterprise/app/review:review_model",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "review_model",
    srcs = ["review_model.ts"],
    deps = [
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

ts_library(
    name = "file_content_monaco",
    srcs = ["file_content_monaco.tsx"],
    deps = [
        "//enterprise/app/review:file_content_service",
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
        "//enterprise/app/review:review_thread",
        "//proto:github_ts_proto",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//react-dom",
        "@npm//tslib",
    ],
)

ts_library(
    name = "file_content_service",
    srcs = ["file_content_service.ts"],
    deps = [
        "//app/service:rpc_service",
        "//app/util:async",
        "//enterprise/app/monaco",
        "//proto:github_ts_proto",
        "@npm//monaco-editor",
        "@npm//tslib",
    ],
)

ts_library(
    name = "pull_request_header",
    srcs = ["pull_request_header.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/link",
        "//app/router",
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
        "//proto:github_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

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
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
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
        "//app/components/button",
        "//app/components/button:checkbox_button",
        "//app/components/dialog",
        "//app/components/link",
        "//app/components/modal",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/review:file_content_monaco",
        "//enterprise/app/review:pull_request_header",
        "//enterprise/app/review:review_model",
        "//enterprise/app/review:review_thread",
        "//proto:github_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
        "@npm//tslib",
    ],
)
