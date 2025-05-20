load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/github",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/schema",
        "//codesearch/types",
        "//proto:index_go_proto",
        "//server/util/git",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
        "@com_github_go_enry_go_enry_v2//:go-enry",
    ],
)

go_test(
    name = "github_test",
    srcs = ["github_test.go"],
    embed = [":github"],
    deps = [
        "//codesearch/index",
        "//codesearch/schema",
        "//proto:index_go_proto",
        "//server/testutil/testfs",
        "//server/util/git",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
