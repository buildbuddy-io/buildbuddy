load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "@buildbuddy_internal//:__subpackages__",
])

go_library(
    name = "wiring",
    srcs = ["wiring.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/server/wiring",
    deps = [
        "//server/real_environment",
    ],
)

go_test(
    name = "wiring_test",
    srcs = ["wiring_test.go"],
    deps = [
        ":wiring",
        "//server/real_environment",
    ],
)
