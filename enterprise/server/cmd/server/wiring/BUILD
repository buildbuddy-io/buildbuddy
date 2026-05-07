load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
