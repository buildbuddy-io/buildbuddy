load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files([
    "product_spotlight.css",
    "spotlights/*",
])

ts_library(
    name = "product_spotlight",
    srcs = ["product_spotlight.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/router",
        "//enterprise/app/product_spotlight:spotlight_card",
        "//enterprise/app/product_spotlight:spotlight_service",
    ],
)

ts_library(
    name = "product_spotlight_detail",
    srcs = ["product_spotlight_detail.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/auth:auth_service",
        "//app/router",
        "//enterprise/app/product_spotlight:spotlight_service",
    ],
)

ts_library(
    name = "spotlight_card",
    srcs = ["spotlight_card.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "spotlight_configs",
    srcs = ["spotlight_configs.tsx"],
)

ts_library(
    name = "spotlight_service",
    srcs = ["spotlight_service.tsx"],
    deps = [
        "//:node_modules/tslib",
        "//enterprise/app/product_spotlight:spotlight_configs",
    ],
)
