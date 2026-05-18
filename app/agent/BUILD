load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "agent_dialog",
    srcs = ["agent_dialog.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/dialog:simple_modal_dialog",
        "//app/errors:error_service",
        "//app/service:rpc_service",
        "//proto:git_ts_proto",
        "//proto:github_ts_proto",
        "//proto:runner_ts_proto",
    ],
)
