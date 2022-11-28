def _extract_bazel_installation_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.attr.out_dir)
    ctx.actions.run_shell(
        outputs = [out_dir],
        inputs = [ctx.executable.bazel],
        command = """
            set -e
            "$BAZEL" --install_base="$OUT_DIR"/.install --output_base="$OUT_DIR"/.output &>/dev/null
            rm -rf "$OUT_DIR"/.output
            mv "$OUT_DIR"/.install/* "$OUT_DIR"/
            rmdir "$OUT_DIR"/.install
        """,
        env = {
            "OUT_DIR": out_dir.path,
            "BAZEL": ctx.executable.bazel.path,
        },
    )
    return [DefaultInfo(files = depset([out_dir]))]

extract_bazel_installation = rule(
    implementation = _extract_bazel_installation_impl,
    attrs = {
        "bazel": attr.label(executable = True, cfg = "exec", mandatory = True, allow_single_file = True),
        "out_dir": attr.string(mandatory = True),
    },
)
