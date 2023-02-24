load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["settings.css"])

ts_library(
    name = "settings",
    srcs = ["settings.tsx"],
    strict = True,
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/button",
        "//app/preferences",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/api_keys",
        "//enterprise/app/org:edit_org",
        "//enterprise/app/org:org_members",
        "//enterprise/app/quota",
        "//enterprise/app/secrets",
        "//enterprise/app/settings:github_link",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "github_link",
    srcs = ["github_link.tsx"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/components/button",
        "//app/components/dialog",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)
