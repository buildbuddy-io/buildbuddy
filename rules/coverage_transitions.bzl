"""Transition helpers used to build tools without coverage instrumentation.

The coverage runner builds every target with coverage enabled, which breaks
Go binaries that are used as helper tools inside tests (they print coverage
warnings and behave like test binaries). The transition below disables coverage
and forces an opt build for those binaries so they behave like normal tools.
"""

def _coverage_free_exec_transition_impl(settings, attr):
    return {
        "//command_line_option:collect_code_coverage": "false",
        "//command_line_option:compilation_mode": "opt",
    }

# Transition that turns off coverage and forces opt in the transitioned config.
coverage_free_exec_transition = transition(
    implementation = _coverage_free_exec_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:collect_code_coverage",
        "//command_line_option:compilation_mode",
    ],
)

def _coverage_free_tool_impl(ctx):
    target = ctx.attr.target
    if type(target) == type([]):
        target = target[0]
    default = target[DefaultInfo]

    runfiles = default.default_runfiles.merge(default.data_runfiles)

    return [
        DefaultInfo(
            files = default.files,
            data_runfiles = runfiles,
            default_runfiles = runfiles,
        ),
    ]

# Wrapper rule that rebuilds the given executable with the coverage-free
# transition, exposing identical runfiles for use in data attributes. The rule
# itself transitions to disable coverage/opt builds, and the wrapped target is
# built in the execution configuration so the resulting binary matches the
# executor platform.
coverage_free_tool = rule(
    implementation = _coverage_free_tool_impl,
    attrs = {
        "target": attr.label(
            cfg = coverage_free_exec_transition,
            executable = True,
            mandatory = True,
            providers = [DefaultInfo],
        ),
    },
)
