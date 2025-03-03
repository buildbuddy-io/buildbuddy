load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filestore",
    srcs = ["filestore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/status",
        "@org_golang_google_api//googleapi",
    ],
)

go_test(
    name = "filestore_test",
    srcs = ["filestore_test.go"],
    deps = [
        ":filestore",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/testutil/testdigest",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
