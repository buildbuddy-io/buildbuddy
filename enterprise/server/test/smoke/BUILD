load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Smoke tests an executor binary (e.g. a release artifact) against an existing
# BuildBuddy app. Must run on a host matching the executor's OS/arch. See
# README.md for usage.
go_test(
    name = "executor_smoke_test",
    size = "large",
    srcs = [
        "executor_smoke_test.go",
        "main_test.go",
    ],
    tags = [
        "manual",
        "requires-network",
    ],
    deps = ["//enterprise/server/test/smoke/executorsmoke"],
)

# Runs the smoke test suite against a test-scoped app. Uses the executor built
# from source unless --executor_binary is passed.
go_test(
    name = "executor_smoke_local_test",
    size = "medium",
    srcs = [
        "executor_smoke_local_test.go",
        "main_test.go",
    ],
    data = ["//enterprise/server/cmd/executor"],
    target_compatible_with = ["@platforms//os:linux"],
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
