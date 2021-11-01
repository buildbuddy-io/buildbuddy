# Based on
# https://github.com/bazelbuild/rules_webtesting/blob/6b47a3f11b7f302c2620a3552cf8eea8855e8c9e/web/internal/wrap_web_test_suite.bzl
#
# Differences:
# - Set exec_properties on the web_test_suite to a docker image that ensures
#   chromium deps are installed
# - Set tags to defaults that make sense for our use case
# - Make shard_count a required arg as a forcing function to make individual
#   tests run in parallel
#
# We can't use go_web_test_suite from rules_webtesting because it doesn't
# propagate exec_properties to the underlying web_test target.

load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("@io_bazel_rules_webtesting//web:web.bzl", "web_test_suite")

DEFAULT_WRAPPED_TEST_TAGS = ("manual", "noci")

DEFAULT_TEST_SUITE_TAGS = ("manual",)

def go_web_test_suite(
        name,
        shard_count = None,
        browsers = None,
        args = None,
        browser_overrides = None,
        config = None,
        flaky = None,
        local = None,
        size = None,
        tags = None,
        test_suite_tags = DEFAULT_TEST_SUITE_TAGS,
        timeout = None,
        visibility = None,
        web_test_data = None,
        wrapped_test_tags = DEFAULT_WRAPPED_TEST_TAGS,
        **kwargs):
    # TODO(bduffany): Generate a test to automatically enforce `shard_count == number of tests`
    if not shard_count:
        fail("shard_count should be set to match the number of tests to ensure that tests are run in parallel.")

    browsers = browsers or [
        "@io_bazel_rules_webtesting//browsers:chromium-local",
    ]
    tags = tags or []
    if "webdriver" not in tags:
        tags.append("webdriver")
    if "native" not in tags:
        tags.append("native")
    size = size or "large"
    wrapped_test_name = name + "_wrapped_test"

    exec_properties = {
        "container-image": "docker://marketplace.gcr.io/google/rbe-ubuntu16-04-webtest@sha256:0b8fa87db4b8e5366717a7164342a029d1348d2feea7ecc4b18c780bc2507059",
    }

    go_test(
        name = wrapped_test_name,
        args = args,
        flaky = flaky,
        local = local,
        shard_count = shard_count,
        size = size,
        tags = wrapped_test_tags,
        timeout = timeout,
        visibility = ["//visibility:private"],
        exec_properties = exec_properties,
        **kwargs
    )

    web_test_suite(
        name = name,
        args = args,
        browsers = browsers,
        config = config,
        data = web_test_data,
        flaky = flaky,
        local = local,
        shard_count = shard_count,
        size = size,
        tags = tags,
        test = wrapped_test_name,
        test_suite_tags = test_suite_tags,
        timeout = timeout,
        visibility = visibility,

        # NOTE: web_test_suite interprets any dict-valued arguments as a
        # browser-overrides map. Example:
        #
        # size = { "chromium-local": "medium", "default": "large" }
        #
        # To pass a dict-valued arg without having it be interpreted as a
        # browser-overrides map, it can be wrapped in a map like so:
        #
        # {"default": { "your_map_key": "your_map_value" }}.
        exec_properties = {"default": exec_properties},
    )
