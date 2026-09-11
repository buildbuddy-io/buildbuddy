load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":test_fixtures.bzl", "release_test_fixtures")

release_test_fixtures()

sh_test(
    name = "artifacts_exist_test",
    srcs = ["artifacts_exist_test.sh"],
    data = [
        ":empty_release.artifacts_exist",
        ":plain_release",
        ":test_release.artifacts_exist",
    ],
    env = {
        "CHECK": "$(location :test_release.artifacts_exist)",
        "EMPTY_CHECK": "$(location :empty_release.artifacts_exist)",
        "PLAIN": "$(location :plain_release)",
    },
)
