load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "index2",
    srcs = [
        "common.go",
        "mmap_bsd.go",
        "mmap_linux.go",
        "mmap_windows.go",
        "read.go",
        "write.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/index2",
    visibility = ["//visibility:public"],
    deps = [
        "//query",
        "//sparse",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_google_uuid//:uuid",
        "@com_github_roaringbitmap_roaring//:roaring",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "index2_test",
    srcs = [
        "read_test.go",
        "write_test.go",
    ],
    embed = [":index2"],
    deps = [
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_roaringbitmap_roaring//:roaring",
    ],
)

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
