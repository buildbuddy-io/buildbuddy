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
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//:node_modules/tslib",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "file_content_monaco",
    srcs = ["file_content_monaco.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/monaco-editor",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//:node_modules/tslib",
        "//enterprise/app/review:file_content_service",
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
        "//enterprise/app/review:review_thread",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "file_content_service",
    srcs = ["file_content_service.ts"],
    deps = [
        "//:node_modules/monaco-editor",
        "//:node_modules/tslib",
        "//app/service:rpc_service",
        "//app/util:async",
        "//enterprise/app/monaco",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "pull_request_header",
    srcs = ["pull_request_header.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/button",
        "//app/components/link",
        "//app/router",
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "review",
    srcs = ["review.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/router",
        "//enterprise/app/review:review_list",
        "//enterprise/app/review:view_pull_request",
    ],
)

ts_library(
    name = "review_list",
    srcs = ["review_list.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/react",
        "//app/components/link",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:react",
        "//proto:github_ts_proto",
    ],
)

ts_library(
    name = "review_thread",
    srcs = ["review_thread.tsx"],
    deps = [
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//enterprise/app/review:review_controller",
        "//enterprise/app/review:review_model",
    ],
)

ts_library(
    name = "view_pull_request",
    srcs = ["view_pull_request.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//app/components/button:checkbox_button",
        "//app/components/dialog",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/review:file_content_monaco",
        "//enterprise/app/review:file_content_service",
        "//enterprise/app/review:pull_request_header",
        "//enterprise/app/review:review_model",
        "//enterprise/app/review:review_thread",
        "//proto:github_ts_proto",
    ],
)
