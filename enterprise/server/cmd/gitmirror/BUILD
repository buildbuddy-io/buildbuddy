load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gitmirror_lib",
    srcs = ["gitmirror.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/gitmirror",
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/gitmirror",
        "//server/config",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_binary(
    name = "gitmirror",
    args = [
        "--git.mirror.root_directory=/tmp/buildbuddy-gitmirror",
        "--port=8180",
        "--monitoring.listen=:9191",
    ],
    embed = [":gitmirror_lib"],
)
