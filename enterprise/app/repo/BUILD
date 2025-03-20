load("//rules/typescript:index.bzl", "ts_library")

exports_files(glob(["*.css"]))

ts_library(
    name = "repo",
    srcs = ["repo.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/icons:github",
        "//app/icons:google",
        "//app/picker:picker_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:popup",
        "//enterprise/app/org_picker",
        "//enterprise/app/secrets:secret_util",
        "//proto:github_ts_proto",
        "//proto:repo_ts_proto",
        "//proto:secrets_ts_proto",
        "//proto:workflow_ts_proto",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
