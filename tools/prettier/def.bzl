def _prettier_runner_impl(ctx):
    if ctx.attr.binary == "check":
        executable = ctx.executable._check
    else:
        executable = ctx.executable._fix
    ctx.actions.run(
        executable = executable,
    )

_prettier_runner = rule(
    implementation = _prettier_runner_impl,
    attrs = {
        "binary": attr.string(
            values = ["fix", "check"],
        ),
        "_fix": attr.label(
            default = ":fix",
            executable = True,
            cfg = "host",
        ),
        "_check": attr.label(
            default = ":check",
            executable = True,
            cfg = "host",
        ),
    },
    executable = True,
)

def prettier(name, **kwargs):
    _prettier_runner(
        name = name + "_fix",
        binary = "fix",
        tags = ["manual"],
        **kwargs
    )
    _prettier_runner(
        name = name + "_check",
        binary = "check",
        tags = ["manual"],
        **kwargs
    )
