load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "index",
    srcs = [
        "common.go",
        "mmap_bsd.go",
        "mmap_linux.go",
        "mmap_windows.go",
        "read.go",
        "write.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/index",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/query",
        "//codesearch/sparse",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_google_uuid//:uuid",
        "@com_github_roaringbitmap_roaring//:go_default_library",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "index_test",
    srcs = [
        "read_test.go",
        "write_test.go",
    ],
    embed = [":index"],
    deps = [
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_roaringbitmap_roaring//:go_default_library",
    ],
)
