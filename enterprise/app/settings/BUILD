load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["settings.css"])

ts_library(
    name = "settings",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/alert",
        "//app/auth",
        "//app/auth:user",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors",
        "//app/preferences",
        "//app/router",
        "//app/service",
        "//enterprise/app/api_keys",
        "//enterprise/app/org",
        "//enterprise/app/quota",
        "//enterprise/app/secrets",
        "//proto:github_ts_proto",
    ],
)
