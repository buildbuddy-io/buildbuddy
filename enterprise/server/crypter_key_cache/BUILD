load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "crypter_key_cache",
    srcs = ["crypter_key_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache",
    deps = [
        "//enterprise/server/crypter",
        "//proto:storage_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_crypto//hkdf",
        "@org_uber_go_atomic//:atomic",
    ],
)
