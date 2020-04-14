load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "db",
    srcs = [
        "db.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/db",
    deps = [
        "//server:config",
        "//server:tables",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@com_github_jinzhu_gorm//dialects/mysql:go_default_library",
        "@com_github_jinzhu_gorm//dialects/postgres:go_default_library",
        "@com_github_jinzhu_gorm//dialects/sqlite:go_default_library",
    ],
)

go_library(
    name = "disk",
    srcs = ["disk.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/disk",
)

go_library(
    name = "healthcheck",
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/healthcheck",
)

go_library(
    name = "perms",
    srcs = ["perms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/perms",
    deps = [
        "//server:tables",
    ],
)

go_library(
    name = "protofile",
    srcs = ["protofile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/protofile",
    deps = [
        "//server:interfaces",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_library(
    name = "query_builder",
    srcs = ["query_builder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/query_builder",
)

go_library(
    name = "random",
    srcs = ["random.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/random",
)

go_library(
    name = "status",
    srcs = [
        "status.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/status",
    deps = [
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)
