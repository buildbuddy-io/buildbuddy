load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# Where the tunnel is unsupported, an empty file satisfies go:embed.
genrule(
    name = "embedsrcs_compatible_helper",
    srcs = select({
        "@io_bazel_rules_go//go/platform:darwin": ["//tools/bbaccess/tunnel/tunhelper"],
        "@io_bazel_rules_go//go/platform:linux": ["//tools/bbaccess/tunnel/tunhelper"],
        "//conditions:default": [],
    }),
    outs = ["tunhelper"],
    cmd_bash = select({
        "@io_bazel_rules_go//go/platform:darwin": "cp $(SRCS) $@",
        "@io_bazel_rules_go//go/platform:linux": "cp $(SRCS) $@",
        "//conditions:default": "touch $@",
    }),
)

go_library(
    name = "install",
    srcs = [
        "install.go",
        "install_darwin.go",
        "install_linux.go",
        "install_other.go",
    ],
    embedsrcs = ["tunhelper"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/install",
    visibility = ["//visibility:public"],
    deps = [
        "//tools/bbaccess/tunnel/tunhelperutil",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "@com_github_mattn_go_isatty//:go-isatty",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//tools/bbaccess/tunnel/daemon",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//tools/bbaccess/tunnel/daemon",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "install_test",
    srcs = [
        "install_darwin_test.go",
        "install_linux_test.go",
        "install_test.go",
    ],
    embed = [":install"],
    target_compatible_with = select({
        "@io_bazel_rules_go//go/platform:darwin": [],
        "@io_bazel_rules_go//go/platform:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = [
        "//tools/bbaccess/tunnel/tunhelperutil",
        "@com_github_stretchr_testify//require",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//tools/bbaccess/tunnel/daemon",
            "//tools/bbaccess/tunnel/tunnelconfig",
        ],
        "//conditions:default": [],
    }),
)
