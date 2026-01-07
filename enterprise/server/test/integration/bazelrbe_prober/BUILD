load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "bazelrbe_prober_test",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-6.5.0_crossplatform",
        "//server/util/bazel:bazel-6.5.0_extract_installation",
        "//server/util/bazel:bazel-7.6.1_crossplatform",
        "//server/util/bazel:bazel-7.6.1_extract_installation",
        "//server/util/bazel:bazel-8.5.0_crossplatform",
        "//server/util/bazel:bazel-8.5.0_extract_installation",
        "//tools/probers/bazelrbe",
    ],
    # Linux-only: the extract_bazel_installation rule fails on Darwin in
    # sandboxed builds because it can't find gcc/CC.
    target_compatible_with = ["@platforms//os:linux"],
    x_defs = {
        "bazel6Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_crossplatform)",
        "outdir6Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_extract_installation)",
        "bazel7Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-7.6.1_crossplatform)",
        "outdir7Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-7.6.1_extract_installation)",
        "bazel8Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_crossplatform)",
        "outdir8Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_extract_installation)",
        "proberRlocationpath": "$(rlocationpath //tools/probers/bazelrbe)",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
