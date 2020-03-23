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
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "cache",
    srcs = ["cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/cache",
    deps = [
        "//server:interfaces",
        "//server:tables",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

go_library(
    name = "database",
    srcs = [
        "database.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/database",
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
    name = "simplesearcher",
    srcs = ["simplesearcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/simplesearcher",
    deps = [
        "//proto:invocation_go_proto",
        "//server:tables",
        "//server/backends:database",
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
