load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Runs the smoke test suite against a test-scoped app. Uses the executor built
# from source unless --executor_binary is passed.
go_test(
    name = "local_test",
    size = "medium",
    srcs = [
        "executor_smoke_local_test.go",
        "main_test.go",
    ],
    data = ["//enterprise/server/cmd/executor"],
    # The test-scoped app server doesn't build for Windows.
    target_compatible_with = select({
        "@platforms//os:windows": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
    x_defs = {
        "executorRlocationpath": "$(rlocationpath //enterprise/server/cmd/executor)",
    },
    deps = [
        "//enterprise/server/test/smoke/executorsmoke",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
