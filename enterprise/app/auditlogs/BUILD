load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["auditlogs.css"])

ts_library(
    name = "auditlogs",
    srcs = ["auditlogs.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-date-range",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/react-date-range",
        "//:node_modules/tslib",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//proto:auditlog_ts_proto",
    ],
)
