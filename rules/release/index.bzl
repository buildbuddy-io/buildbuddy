load("@rules_multirun//:defs.bzl", "command", "multirun")

# Creates a release step that depends on another release step.
#
# When run, it will run the target specified by the `run` argument
# after the target specifed by the `after` argument.
#
# Example usage:
# ```
#    release(
#        name = "dev",
#        run = ":k8s_dev"
#        after = ":app_release_dev",
#    )
# ```
#
# Then to perform a release, run:
#   `bazel run :dev.apply`
#
# To diff a release, run:
#   `bazel run :dev.diff`
#
# To delete a release, run:
#   `bazel run :dev.delete`
#
def release(name, run, after, enable_actions = True, **kwargs):
    actions = [""]
    if enable_actions:
        actions = [".apply", ".diff", ".delete"]

    for action in actions:
        after_action_command = name + action + ".after"
        run_action_command = name + action + ".run"
        command(
            name = after_action_command,
            command = after + action,
        )
        command(
            name = run_action_command,
            command = run + action,
        )
        multirun(
            name = name + action,
            commands = [
                after_action_command,
                run_action_command,
            ],
            **kwargs
        )
