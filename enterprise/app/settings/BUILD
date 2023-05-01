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
        "//app/components/link",
        "//app/preferences",
        "//app/router",
        "//app/service:rpc_service",
        "//enterprise/app/api_keys",
        "//enterprise/app/encryption",
        "//enterprise/app/org:edit_org",
        "//enterprise/app/org:org_members",
        "//enterprise/app/quota",
        "//enterprise/app/secrets",
        "//enterprise/app/settings:github_complete_installation",
        "//enterprise/app/settings:github_link",
        "//enterprise/app/settings:user_github_link",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
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
        "//app/capabilities",
        "//app/components/banner",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/dialog",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/link",
        "//app/components/modal",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "user_github_link",
    srcs = ["user_github_link.tsx"],
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
        "//:node_modules/@octokit/rest",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "github_complete_installation",
    srcs = ["github_complete_installation.tsx"],
    strict = True,
    deps = [
        "//app/alert:alert_service",
        "//app/auth:auth_service",
        "//app/auth:user",
        "//app/components/banner",
        "//app/components/dialog:simple_modal_dialog",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:github_ts_proto",
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/react",
    ],
)
