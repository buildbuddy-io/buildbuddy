load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# Smoke tests an executor binary (e.g. a release artifact) against an existing
# BuildBuddy app. Must run on a host matching the executor's OS/arch. See
# README.md for usage.
go_test(
    name = "smoke_test",
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
