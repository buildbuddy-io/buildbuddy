load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "quota",
    srcs = ["quota.tsx"],
    deps = [
        "//enterprise/app/quota:bucket",
        "//enterprise/app/quota:namespace",
        "//enterprise/app/quota:namespaces",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "bucket",
    srcs = ["bucket.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/quota:bucket_form",
        "//enterprise/app/quota:quota_breadcrumbs",
        "//proto:quota_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "bucket_form",
    srcs = ["bucket_form.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/input",
        "//proto:duration_ts_proto",
        "//proto:quota_ts_proto",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "namespace",
    srcs = ["namespace.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/quota:quota_breadcrumbs",
        "//proto:quota_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "namespaces",
    srcs = ["namespaces.tsx"],
    deps = [
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:quota_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "quota_breadcrumbs",
    srcs = ["quota_breadcrumbs.tsx"],
    deps = [
        "//app/components/link",
        "@npm//@types/react",
        "@npm//react",
    ],
)
