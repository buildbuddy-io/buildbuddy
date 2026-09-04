load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["auditlogs.css"])

ts_library(
    name = "auditlogs",
    srcs = ["auditlogs.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/date-fns",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/components/breadcrumbs",
        "//app/components/button",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//enterprise/app/filter:date_range_picker_button",
        "//enterprise/app/filter:filter_util",
        "//proto:auditlog_ts_proto",
    ],
)
