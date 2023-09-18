load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["sidebar.css"])

ts_library(
    name = "sidebar",
    srcs = ["sidebar.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/link",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:group_ts_proto",
        "//proto:user_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)
