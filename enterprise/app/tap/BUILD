load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = ["tap.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:git",
        "//app/util:math",
        "//proto:duration_ts_proto",
        "//proto:invocation_ts_proto",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//react-dom",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)
