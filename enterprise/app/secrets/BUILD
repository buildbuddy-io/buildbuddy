load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["secrets.css"])

ts_library(
    name = "secrets",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/libsodium-wrappers",
        "//:node_modules/@types/react",
        "//:node_modules/libsodium-wrappers",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/alert",
        "//app/components/button",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/input",
        "//app/components/link",
        "//app/components/spinner",
        "//app/errors",
        "//app/router",
        "//app/service",
        "//proto:secrets_ts_proto",
    ],
)
