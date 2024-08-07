load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = ["tap.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:git",
        "//enterprise/app/tap:flakes",
        "//enterprise/app/tap:grid",
        "//enterprise/app/tap:grid_sort_controls",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "tap_empty_state",
    srcs = ["tap_empty_state.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "grid",
    srcs = ["grid.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/filter_input",
        "//app/components/spinner",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:math",
        "//enterprise/app/tap:grid_common",
        "//enterprise/app/tap:tap_empty_state",
        "//proto:duration_ts_proto",
        "//proto:target_ts_proto",
        "//proto/api/v1:common_ts_proto",
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//react-dom",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "grid_common",
    srcs = ["grid_common.ts"],
)

ts_library(
    name = "grid_sort_controls",
    srcs = ["grid_sort_controls.tsx"],
    deps = [
        "//app/components/select",
        "//app/router",
        "//enterprise/app/tap:grid_common",
        "@npm//@types/react",
        "@npm//react",
    ],
)

ts_library(
    name = "flakes",
    srcs = ["flakes.tsx"],
    deps = [
        "//app/components/banner",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/components/select",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/target:target_flaky_test_card",
        "//app/util:async",
        "//enterprise/app/tap:tap_empty_state",
        "//enterprise/app/trends:trends_chart",
        "//proto:target_ts_proto",
        "@npm//@types/moment",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//moment",
        "@npm//react",
        "@npm//tslib",
    ],
)
