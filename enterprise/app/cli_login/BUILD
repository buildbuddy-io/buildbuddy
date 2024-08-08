load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "cli_login",
    srcs = ["cli_login.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

exports_files(["cli_login.css"])
