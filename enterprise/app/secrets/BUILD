load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["secrets.css"])

ts_library(
    name = "secrets",
    srcs = ["secrets.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//enterprise/app/secrets:secrets_list",
        "//enterprise/app/secrets:update_secret",
    ],
)

ts_library(
    name = "secrets_list",
    srcs = ["secrets_list.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/link",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:secrets_ts_proto",
    ],
)

ts_library(
    name = "update_secret",
    srcs = ["update_secret.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/path-browserify",  # keep
        "//:node_modules/react",
        "//app/alert:alert_service",
        "//app/components/button",
        "//app/components/input",
        "//app/components/link",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//enterprise/app/secrets:secret_util",
    ],
)

ts_library(
    name = "secret_util",
    srcs = ["secret_util.ts"],
    deps = [
        "//:node_modules/@types/libsodium-wrappers",
        "//:node_modules/libsodium-wrappers",
        "//app/service:rpc_service",
        "//proto:secrets_ts_proto",
    ],
)
