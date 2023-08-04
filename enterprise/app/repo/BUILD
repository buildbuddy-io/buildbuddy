load("//rules/typescript:index.bzl", "ts_library")

exports_files(glob(["*.css"]))

ts_library(
    name = "repo",
    srcs = ["repo.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/checkbox",
        "//app/components/input",
        "//app/components/select",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//enterprise/app/secrets:secret_util",
        "//proto:github_ts_proto",
        "//proto:repo_ts_proto",
        "//proto:secrets_ts_proto",
        "//proto:workflow_ts_proto",
        "@npm//@octokit/rest",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
