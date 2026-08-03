# Temporary demo package for exercising flaky-test drag analysis.
# flaky_test always fails; run with --flaky_test_attempts to produce
# retry_of chains in the execution graph log.

load("@rules_shell//shell:sh_test.bzl", "sh_test")

sh_test(
    name = "flaky_test",
    srcs = ["flaky_test.sh"],
)

sh_test(
    name = "passing_test",
    srcs = ["passing_test.sh"],
)
