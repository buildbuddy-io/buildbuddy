load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "history",
    srcs = ["history.tsx"],
    deps = [
        "//app/auth:auth_service",
        "//app/capabilities",
        "//app/components/button",
        "//app/components/button:link_button",
        "//app/components/link",
        "//app/components/spinner",
        "//app/components/tooltip",
        "//app/format",
        "//app/router",
        "//app/router:router_params",
        "//app/service:rpc_service",
        "//app/shortcuts",
        "//enterprise/app/filter",
        "//enterprise/app/filter:filter_util",
        "//enterprise/app/history:history_invocation_card",
        "//enterprise/app/history:history_invocation_stat_card",
        "//enterprise/app/org:org_join_requests",
        "//proto:invocation_status_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

ts_library(
    name = "history_invocation_card",
    srcs = ["history_invocation_card.tsx"],
    deps = [
        "//app/components/link",
        "//app/format",
        "//app/router",
        "//app/util:exit_codes",
        "//proto:invocation_status_ts_proto",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "history_invocation_stat_card",
    srcs = ["history_invocation_stat_card.tsx"],
    deps = [
        "//app/format",
        "//app/router",
        "//proto:invocation_ts_proto",
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
    ],
)

ts_library(
    name = "history_tabs",
    srcs = ["history_tabs.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
    ],
)
