load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["sidebar.css"])

ts_library(
    name = "sidebar",
    srcs = ["sidebar.tsx"],
    strict = True,
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/link",
        "//app/router",
        "//app/service:rpc_service",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)
