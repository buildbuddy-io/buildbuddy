load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":index.bzl", "release")

# Supply the component actions expected by release(); only artifacts_exist is
# exercised by this test.
[
    sh_binary(
        name = "test_" + component + action,
        testonly = True,
        srcs = ["predicate.sh"],
        args = [component],
        tags = ["manual"],
    )
    for component in [
        "after",
        "run",
    ]
    for action in [
        ".apply",
        ".diff",
        ".delete",
        ".push_only",
        ".apply_only",
        ".artifacts_exist",
    ]
]

release(
    name = "test_release",
    testonly = True,
    after = ":test_after",
    run = ":test_run",
    tags = ["manual"],
)

sh_test(
    name = "artifacts_exist_test",
    size = "small",
    srcs = ["artifacts_exist_test.sh"],
    data = [":test_release.artifacts_exist"],
    env = {"CHECK": "$(location :test_release.artifacts_exist)"},
)
