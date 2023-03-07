def _extract_bazel_installation_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.attr.out_dir)
    ctx.actions.run_shell(
        outputs = [out_dir],
        inputs = [ctx.executable.bazel],
        command = """
            USER=nobody "$BAZEL" --output_user_root="$OUT_DIR/.output_user_root" --install_base="$OUT_DIR"/.install --output_base="$OUT_DIR"/.output &>action.log
            code="$?"
            trap 'rm action.log' EXIT
            if (( code )); then
                cat action.log >&2
                exit "$code"
            fi

            set -e
            rm -rf "$OUT_DIR"/.output_user_root
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
