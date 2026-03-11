load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//rules/flags:index.bzl", "write_flag_to_file")

package(default_visibility = ["//cli:__subpackages__"])

string_flag(
    name = "cli_version",
    build_setting_default = "unknown",
)

write_flag_to_file(
    name = "version_flag",
    out = "version_flag.txt",
    flag = ":cli_version",
)

genrule(
    name = "stamped_version",
    outs = ["stamped_version.txt"],
    cmd_bash = select({
        "//:fastbuild": "printf 'unknown' > $@",
        "//conditions:default": """
            version=$$(grep ^STABLE_VERSION_TAG bazel-out/stable-status.txt 2>/dev/null | cut -d' ' -f2); \
            if [ -z "$$version" ]; then version=unknown; fi; \
            printf '%s' $$version > $@;
        """,
    }),
    stamp = 1,
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = [
        ":version_flag",
        ":stamped_version",
    ],
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
