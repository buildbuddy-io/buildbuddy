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
        native.genrule(
            name = name + action,
            exec_tools = [run + action, after + action],
            outs = [name + action + ".out"],
            cmd = "echo \"bash $(location %s) && bash $(location %s);\" > $@" % (after + action, run + action),
            local = 1,
            executable = 1,
            **kwargs
        )
