load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "object",
    srcs = ["object.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/spinner",
        "//app/util:errors",
        "//enterprise/atlas/app/components:badges",
        "//enterprise/atlas/app/components:copy_button",
        "//enterprise/atlas/app/components:link",
        "//enterprise/atlas/app/lib:format",
        "//enterprise/atlas/app/lib:router",
        "//enterprise/atlas/app/lib:rpc_service",
        "//enterprise/atlas/app/object:logs",
        "//proto:atlas_ts_proto",
    ],
)

ts_library(
    name = "logs",
    srcs = ["logs.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/components/button",
        "//app/components/checkbox",
        "//app/components/select",
        "//app/util:errors",
        "//enterprise/atlas/app/lib:rpc_service",
        "//enterprise/atlas/app/lib:structured_log",
        "//proto:atlas_ts_proto",
    ],
)
