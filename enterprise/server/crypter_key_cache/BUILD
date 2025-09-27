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
        "//server/metrics",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@org_uber_go_atomic//:atomic",
    ],
)

go_test(
    name = "crypter_key_cache_test",
    timeout = "short",
    srcs = ["crypter_key_cache_test.go"],
    deps = [
        ":crypter_key_cache",
        "//proto:storage_go_proto",
        "//server/real_environment",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
