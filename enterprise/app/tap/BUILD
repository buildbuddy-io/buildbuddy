load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["tap.css"])

ts_library(
    name = "tap",
    srcs = ["tap.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/select",
        "//app/errors:error_service",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:git",
        "//enterprise/app/filter:date_picker_button",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/tap:flakes",
        "//enterprise/app/tap:grid",
        "//enterprise/app/tap:grid_sort_controls",
        "//proto:invocation_ts_proto",
    ],
)

ts_library(
    name = "tap_empty_state",
    srcs = ["tap_empty_state.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "grid",
    srcs = ["grid.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
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
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//app/components/select",
        "//app/router",
        "//enterprise/app/tap:grid_common",
    ],
)

ts_library(
    name = "flakes",
    srcs = ["flakes.tsx"],
    deps = [
        "//:node_modules/@types/moment",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/moment",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/components/banner",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/components/select",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/target:target_flaky_test_card",
        "//app/target:target_test_log_card",
        "//app/util:async",
        "//app/util:clipboard",
        "//app/util:proto",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/tap:tap_empty_state",
        "//enterprise/app/trends:trends_chart",
        "//proto:target_ts_proto",
    ],
)
