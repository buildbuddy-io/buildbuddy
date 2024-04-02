load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

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
    doc = "Extract Bazel's executable zip file into install_base directory and cache it to reuse later",
)

def bazel_pkg_tar(name, versions = [], **kwargs):
    """Create a tar file containing Bazel executable for each version in versions."""
    for version in versions:
        native.genrule(
            name = "bazel-{}_crossplatform".format(version),
            srcs = select({
                "@bazel_tools//src/conditions:darwin": ["@io_bazel_bazel-{}-darwin-x86_64//file:downloaded".format(version)],
                "//platforms/configs:linux_arm64": ["@io_bazel_bazel-{}-linux-arm64//file:downloaded".format(version)],
                "//conditions:default": ["@io_bazel_bazel-{}-linux-x86_64//file:downloaded".format(version)],
            }),
            outs = ["bazel-{}".format(version)],
            cmd_bash = "cp $(SRCS) $@",
            executable = True,
            **kwargs
        )
        extract_bazel_installation(
            name = "bazel-{}_extract_installation".format(version),
            bazel = ":bazel-{}_crossplatform".format(version),
            out_dir = "bazel-{}_install".format(version),
            **kwargs
        )
    pkg_tar(
        name = name,
        srcs = [":bazel-{}_crossplatform".format(version) for version in versions],
        package_dir = "/bazel",
        **kwargs
    )
