load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//rules:platform_transitions.bzl", "linux_x86_64_alias")

# These stamped files are platform-independent, so pin them to one target
# platform instead of regenerating them once per consuming platform.

genrule(
    name = "populate_version_impl",
    outs = ["version.txt"],
    cmd_bash = select({
        "//:fastbuild": "touch $@",
        "//conditions:default": """
            version=$$(grep ^STABLE_VERSION_TAG bazel-out/stable-status.txt 2>/dev/null | cut -d' ' -f2); \
            if [ -z "$$version" ]; then version=dev; fi; \
            printf '%s' $$version > $@;
        """,
    }),
    stamp = 1,
    tags = ["manual"],
)

linux_x86_64_alias(
    name = "populate_version",
    actual = ":populate_version_impl",
)

genrule(
    name = "populate_commit_impl",
    outs = ["commit.txt"],
    cmd_bash = select({
        "//:fastbuild": "touch $@",
        "//conditions:default": """
            commit=$$(grep ^STABLE_COMMIT_SHA bazel-out/stable-status.txt 2>/dev/null | cut -d' ' -f2); \
            if [ -z "$$commit" ]; then commit=dev; fi; \
            printf '%s' $$commit > $@;
        """,
    }),
    stamp = 1,
    tags = ["manual"],
)

linux_x86_64_alias(
    name = "populate_commit",
    actual = ":populate_commit_impl",
)

genrule(
    name = "populate_latest_public_release_impl",
    outs = ["latest_public_release.txt"],
    cmd_bash = select({
        "//:fastbuild": "touch $@",
        "//conditions:default": """
            release=$$(grep ^STABLE_LATEST_PUBLIC_RELEASE bazel-out/stable-status.txt 2>/dev/null | cut -d' ' -f2); \
            printf '%s' $$release > $@;
        """,
    }),
    stamp = 1,
    tags = ["manual"],
)

linux_x86_64_alias(
    name = "populate_latest_public_release",
    actual = ":populate_latest_public_release_impl",
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = [
        ":populate_commit",
        ":populate_latest_public_release",
        ":populate_version",
    ],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "version_test",
    srcs = ["version_test.go"],
    embed = [":version"],
    deps = ["@com_github_stretchr_testify//require"],
)
