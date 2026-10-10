load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["cache_proxies.css"])

ts_library(
    name = "cache_proxies",
    srcs = ["cache_proxies.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/breadcrumbs",
        "//app/components/button:link_button",
        "//app/components/upgrade",
        "//app/components/view_mode_toggle",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//enterprise/app/cache_proxies:cache_proxy_card",
        "//proto:api_key_ts_proto",
        "//proto:cache_proxy_ts_proto",
        "//proto:capability_ts_proto",
    ],
)

ts_library(
    name = "cache_proxy",
    srcs = ["cache_proxy.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/components/breadcrumbs",
        "//app/components/chart:donut_chart",
        "//app/components/filter_input",
        "//app/components/link",
        "//app/format",
        "//app/router",
        "//app/service:rpc_service",
        "//app/util:errors",
        "//enterprise/app/cache_proxies:cache_proxy_card",
        "//proto:cache_proxy_ts_proto",
    ],
)

ts_library(
    name = "cache_proxy_card",
    srcs = ["cache_proxy_card.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/react",
        "//:node_modules/long",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/recharts",
        "//:node_modules/tslib",
        "//app/components/link",
        "//app/format",
        "//proto:cache_proxy_ts_proto",
        "//proto:timestamp_ts_proto",
    ],
)
