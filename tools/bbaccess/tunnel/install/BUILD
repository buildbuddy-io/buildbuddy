load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "install",
    srcs = [
        "install.go",
        "install_darwin.go",
        "install_linux.go",
        "install_other.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/install",
    visibility = ["//visibility:public"],
    # Flat rather than the per-platform select Gazelle generates from the
    # build constraints: both packages build everywhere, and the stub for
    # other platforms simply uses fewer of them.
    deps = [
        "//tools/bbaccess/tunnel/daemon",
        "//tools/bbaccess/tunnel/tunnelconfig",
    ],  # keep
)

go_test(
    name = "install_test",
    srcs = ["install_linux_test.go"],
    embed = [":install"],
    target_compatible_with = select({
        "@io_bazel_rules_go//go/platform:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
