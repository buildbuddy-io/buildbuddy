load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "populate_version",
    outs = ["version.txt"],
    cmd_bash = """
        version=$$(grep ^STABLE_VERSION_TAG bazel-out/stable-status.txt | cut -d' ' -f2); \
        printf '%s' $$version > $@;
    """,
    stamp = 1,
)

genrule(
    name = "populate_commit",
    outs = ["commit.txt"],
    cmd_bash = """
        commit=$$(grep ^STABLE_COMMIT_SHA bazel-out/stable-status.txt | cut -d' ' -f2); \
        printf '%s' $$commit > $@;
    """,
    stamp = 1,
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = [
        "version.txt",
        "commit.txt",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)
