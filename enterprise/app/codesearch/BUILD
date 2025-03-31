load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["codesearch.css"])

ts_library(
    name = "codesearch",
    srcs = ["codesearch.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/input",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "//app/util:errors",
        "//enterprise/app/codesearch:result",
        "//proto:search_ts_proto",
    ],
)

ts_library(
    name = "result",
    srcs = ["result.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/button",
        "//proto:search_ts_proto",
    ],
)
