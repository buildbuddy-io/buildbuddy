load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tables",
    srcs = ["tables.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/tables",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:capability_go_proto",
        "//proto:group_go_proto",
        "//proto:usage_go_proto",
        "//proto:user_id_go_proto",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
        "//server/util/uuid",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "tables_test",
    srcs = ["tables_test.go"],
    args = [
        "--testenv.database_type=mysql",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "mysql",
    },
    tags = ["docker"],
    deps = [
        ":tables",
        "//server/test_buddy/identity",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//schema",
    ],
)
