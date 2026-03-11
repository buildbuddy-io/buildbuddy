load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
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
    name = "populate_version",
    srcs = [":version_flag"],
    outs = ["version.txt"],
    cmd_bash = """
        version=$$(cat $(location :version_flag)); \
        if [ -z "$$version" ] || [ "$$version" = "unknown" ]; then \
            stable_status_file="$${BAZEL_STABLE_STATUS_FILE:-}"; \
            if [ -n "$$stable_status_file" ]; then \
                version=$$(grep ^STABLE_CLI_VERSION "$$stable_status_file" 2>/dev/null | cut -d' ' -f2); \
            fi; \
        fi; \
        if [ -z "$$version" ]; then version=unknown; fi; \
        printf '%s' "$$version" > $@
    """,
    stamp = -1,
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = [":populate_version"],
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

go_binary(
    name = "version_release_probe",
    srcs = ["version_release_probe.go"],
    deps = [":version"],
    tags = ["manual"],
)

sh_test(
    name = "version_release_test",
    srcs = ["version_release_test.sh"],
    args = [
        "$(location :version_release_probe)",
        "$(location :populate_version)",
    ],
    data = [
        ":populate_version",
        ":version_release_probe",
    ],
    tags = ["manual"],
)
