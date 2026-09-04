load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "maxmind",
    srcs = ["maxmind.go"],
    embedsrcs = ["dbip-asn-lite.mmdb.gz"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/maxmind",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_oschwald_maxminddb_golang_v2//:maxminddb-golang",
    ],
)

# rules_go requires embedsrcs to live under this package directory, so copy the
# database out of the external repo created by the dbip_asn_lite http_file.
genrule(
    name = "dbip_asn_lite_db",
    srcs = ["@dbip_asn_lite//file:dbip-asn-lite.mmdb.gz"],
    outs = ["dbip-asn-lite.mmdb.gz"],
    cmd = "cp $(SRCS) $@",
)

go_test(
    name = "maxmind_test",
    srcs = ["maxmind_test.go"],
    embed = [":maxmind"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
