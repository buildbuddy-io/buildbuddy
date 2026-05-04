load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "explain_test",
    srcs = ["explain_test.go"],
    data = ["//cli/test/integration/compact:testdata"],
    deps = [
        "//cli/testutil/testcli",
        "//proto:spawn_diff_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
    ],
)
