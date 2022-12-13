def _run_shell_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.write(out, "#!/usr/bin/env bash\n" + ctx.attr.script, is_executable = True)
    return [DefaultInfo(files = depset([out]), executable = out)]

run_shell = rule(
    doc = """
    Declares a runnable shell script target.

    This is more lightweight than sh_binary, so it is well-suited
    as a build or run target for integration tests that run bazel.
    """,
    implementation = _run_shell_impl,
    attrs = {
        "script": attr.string(),
    },
    executable = True,
)
