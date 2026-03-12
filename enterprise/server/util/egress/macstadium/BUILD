load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["macstadium.csv"])

go_binary(
    name = "macstadium",
    embed = [":macstadium_lib"],
    visibility = ["//enterprise/server/util/egress:__pkg__"],
)

go_library(
    name = "macstadium_lib",
    srcs = ["macstadium.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/macstadium",
)
