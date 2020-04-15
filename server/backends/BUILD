load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "blobstore",
    srcs = ["blobstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore",
    deps = [
        "//server:config",
        "//server:interfaces",
        "//server/util:disk",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "cachedb",
    srcs = [
        "cachedb.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/cachedb",
    deps = [
        "//server:tables",
        "//server/util:db",
        "//server/util:status",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)

go_library(
    name = "disk_cache",
    srcs = ["disk_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache",
    deps = [
        "//server:interfaces",
        "//server:tables",
        "//server/util:disk",
        "//server/util:status",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "invocationdb",
    srcs = [
        "invocationdb.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb",
    deps = [
        "//server:environment",
        "//server:tables",
        "//server/util:db",
        "//server/util:perms",
        "//server/util:query_builder",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)

go_library(
    name = "memory_cache",
    srcs = ["memory_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache",
    deps = [
        "//server/util:status",
    ],
)

go_library(
    name = "slack",
    srcs = ["slack.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/slack",
    deps = [
        "//proto:invocation_go_proto",
    ],
)
