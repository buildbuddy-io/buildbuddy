load("@rules_multirun//:defs.bzl", "multirun")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load(":index.bzl", "release")

def release_test_fixtures():
    actions = [".apply", ".diff", ".delete", ".push_only", ".apply_only", ".artifacts_exist"]
    for branch in ["after", "run"]:
        for action in actions + [""]:
            sh_binary(
                name = "test_" + branch + action,
                srcs = ["predicate.sh"],
                args = [branch, action if action else "plain"],
                tags = ["manual"],
                testonly = True,
            )

        # A release step with no artifacts already uses an empty multirun.
        for action in actions:
            multirun(
                name = "empty_" + branch + action,
                commands = [],
                tags = ["manual"],
                testonly = True,
            )

    release(
        name = "test_release",
        after = ":test_after",
        run = ":test_run",
        tags = ["manual"],
        testonly = True,
    )
    release(
        name = "empty_release",
        after = ":empty_after",
        run = ":empty_run",
        tags = ["manual"],
        testonly = True,
    )
    release(
        name = "plain_release",
        after = ":test_after",
        run = ":test_run",
        enable_actions = False,
        tags = ["manual"],
        testonly = True,
    )
    for action in actions:
        if native.existing_rule("plain_release" + action) != None:
            fail("enable_actions=False unexpectedly generated suffix " + action)
