load("//rules/typescript:index.bzl", "ts_library")

exports_files(["auditlogs.css"])

ts_library(
    name = "auditlogs",
    srcs = ["auditlogs.tsx"],
    deps = [
        "//app/components/button",
        "//app/components/popup",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/service:rpc_service",
        "//app/util:proto",
        "//proto:auditlog_ts_proto",
        "@npm//@types/react",
        "@npm//@types/react-date-range",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//react-date-range",
        "@npm//tslib",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
