load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tables",
    srcs = ["tables.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/tables",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/util/log",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
        "//server/util/uuid",
        "@io_gorm_gorm//:gorm",
    ],
)
