load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "product_spotlight.css",
    "spotlights/*",
])

ts_library(
    name = "product_spotlight",
    srcs = [
        "product_spotlight.tsx",
        "product_spotlight_detail.tsx",
        "spotlight_card.tsx",
        "spotlight_service.tsx",
    ],
    deps = [
        "//app/auth:auth_service",
        "//proto:user_ts_proto",
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)
