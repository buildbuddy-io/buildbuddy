load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

genrule(
    name = "populate_version",
    outs = ["version.txt"],
    cmd_bash = select({
        "//:fastbuild": "touch $@",
        "//conditions:default": """
            version=$$(grep ^STABLE_CLI_VERSION_TAG bazel-out/stable-status.txt 2>/dev/null | cut -d' ' -f2 || true); \
            if [ -z "$$version" ]; then version=unknown; fi; \
            printf '%s' "$$version" > $@;
        """,
    }),
    stamp = 1,
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = ["version.txt"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
)

go_test(
    name = "version_test",
    srcs = ["version_test.go"],
    deps = [
        ":version",
        "@com_github_stretchr_testify//require",
    ],
)
