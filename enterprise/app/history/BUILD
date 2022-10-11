load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "history",
    srcs = glob(["*.tsx"]),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/protobufjs",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//app/auth",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/link",
        "//app/components/spinner",
        "//app/components/tooltip",
        "//app/docs",
        "//app/format",
        "//app/router",
        "//app/service",
        "//enterprise/app/filter",
        "//enterprise/app/org",
        "//proto:invocation_ts_proto",
        "//proto:user_ts_proto",
    ],
)
