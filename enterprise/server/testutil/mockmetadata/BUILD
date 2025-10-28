load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "mockmetadata",
    srcs = ["mockmetadata.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/mockmetadata",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/filestore",
        "//proto:metadata_go_proto",
        "//proto:storage_go_proto",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/proto",
        "//server/util/status",
    ],
)

go_test(
    name = "mockmetadata_test",
    srcs = ["mockmetadata_test.go"],
    deps = [
        ":mockmetadata",
        "//enterprise/server/filestore",
        "//proto:metadata_go_proto",
        "//proto:storage_go_proto",
        "//server/testutil/testdigest",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
    ],
)
