load("@build_bazel_rules_nodejs", "run_node")

def _prettier_runner_impl(ctx):
    prettier_args = ""
    if ctx.attr.mode == "" or ctx.attr.mode == "fix":
        prettier_args = "--write"
    if ctx.attr.mode == "check":
        prettier_args = "--check"

    out_file = ctx.actions.declare_file(ctx.label.name + ".bash")
    ctx.actions.run_shell(
        outputs = [out_file],
        command = """
        printf "#!/bin/bash
        set -e
        if [ -z \"\\$BUILD_WORKSPACE_DIRECTORY\" ]; then
          echo 'error: BUILD_WORKSPACE_DIRECTORY not set' >&2
          exit 1
        fi

        \"$PRETTIER_BIN_PATH\" --config \"\\$BUILD_WORKSPACE_DIRECTORY/.prettierrc\" $PRETTIER_ARGS $PRETTIER_PATTERNS
        " >"$OUT_FILE"
        chmod +x "$OUT_FILE"
        """,
        env = {
            "PRETTIER_PATTERNS": " ".join([("\"$BUILD_WORKSPACE_DIRECTORY\"'/%s'" % pattern) for pattern in ctx.attr.patterns]),
            "PRETTIER_BIN_PATH": "$BUILD_WORKSPACE_DIRECTORY/" + ctx.executable.prettier.path,
            "PRETTIER_ARGS": prettier_args,
            "OUT_FILE": out_file.path,
        },
    )
    runfiles = ctx.runfiles(files = [
        ctx.executable.prettier,
        ctx.file._runfiles_script,
    ])
    return [DefaultInfo(
        files = depset([out_file]),
        runfiles = runfiles,
        executable = out_file,
    )]

_prettier_runner = rule(
    implementation = _prettier_runner_impl,
    attrs = {
        "prettier": attr.label(
            default = "@npm//:node",
            executable = True,
            cfg = "host",
        ),
        "patterns": attr.string_list(
            doc = "file/dir/glob (relative to the workspace root) to pass to prettier",
            default = ["**/*.*"],
        ),
        "mode": attr.string(
            values = ["", "fix", "check"],
            default = "",
        ),
    },
    executable = True,
)

def prettier(name, **kwargs):
    _prettier_runner(
        name = name + "_fix",
        mode = "fix",
        tags = ["manual"],
        **kwargs
    )
    _prettier_runner(
        name = name = "_check",
        mode = "check",
        tags = ["manual"],
        **kwargs
    )
    native.sh_binary(
        name = name,
        srcs = [runner_name],
        tags = ["manual"],
    )
