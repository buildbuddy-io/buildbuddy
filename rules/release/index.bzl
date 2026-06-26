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
        run_action_command = name + action + ".run"
        after_action_command = name + action + ".after"
        command(
            name = run_action_command,
            command = run + action,
        )
        command(
            name = after_action_command,
            command = after + action,
        )
        multirun(
            name = name + action,
            commands = [
                run_action_command,
                after_action_command,
            ],
        )
