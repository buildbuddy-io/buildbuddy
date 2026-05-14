load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "login",
    srcs = ["login.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/input",
        "//app/errors:error_service",
        "//app/icons:github",
        "//app/icons:google",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:popup",
        "//proto:group_ts_proto",
    ],
)
