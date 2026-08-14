load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["error_tracking.css"])

ts_library(
    name = "error_tracking",
    srcs = ["error_tracking.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/components/breadcrumbs",
        "//app/components/button",
        "//app/components/link",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/router",
        "//app/service:rpc_service",
        "//proto:error_tracking_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "error_tracking_test",
    srcs = ["error_tracking_test.tsx"],
    deps = [
        ":error_tracking",
        "//:node_modules/long",
        "//app/service:rpc_service",
        "//proto:error_tracking_ts_proto",
    ],
)
