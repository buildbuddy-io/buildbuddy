load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "compare_invocations",
    srcs = ["compare_invocations.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/components/button:checkbox_button",
        "//app/components/button:link_button",
        "//app/invocation:invocation_model",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:invocation_ts_proto",
        "@npm//@types/diff-match-patch",
        "@npm//@types/react",
        "@npm//diff-match-patch",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)
