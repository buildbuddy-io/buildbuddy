load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "find_missing_view_test",
    srcs = ["find_missing_view_test.go"],
    deps = [
        "//enterprise/server/util/pebble",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_cockroachdb_pebble//vfs",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/protowire",
    ],
)
