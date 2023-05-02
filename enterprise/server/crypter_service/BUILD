load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "crypter_service",
    srcs = ["crypter_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_service",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:encryption_go_proto",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_crypto//chacha20poly1305",
        "@org_golang_x_crypto//hkdf",
        "@org_golang_x_sync//singleflight",
        "@org_golang_x_time//rate",
        "@org_uber_go_atomic//:atomic",
    ],
)

go_test(
    name = "crypter_service_test",
    srcs = ["crypter_service_test.go"],
    embed = [":crypter_service"],
    deps = [
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:encryption_go_proto",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/ioutil",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
