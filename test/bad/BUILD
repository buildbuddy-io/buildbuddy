load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "bad",
    srcs = ["test.go"],
    deps = ["@com_github_stretchr_testify//require"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "Pool": "firecracker-arm64-test-gcp",
        "dockerNetwork": "off",
    },
)
