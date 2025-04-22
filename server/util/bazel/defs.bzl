load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

def _extract_bazel_installation_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.attr.out_dir)
    ctx.actions.run_shell(
        outputs = [out_dir],
        inputs = [ctx.executable.bazel],
        command = """
            set -e

            BAZEL="$PWD/$BAZEL"
            OUT_DIR="$PWD/$OUT_DIR"
            WORKDIR="$PWD/_extract_bazel_installation_tmp"

            INSTALL_BASE="$OUT_DIR/install_base"
            REPOSITORY_CACHE="$OUT_DIR/repository_cache"
            OUTPUT_BASE="$WORKDIR/output_base"
            OUTPUT_USER_ROOT="$WORKDIR/output_user_root"

            mkdir -p "$WORKDIR"
            cd "$WORKDIR"
            trap 'rm -rf "$WORKDIR"' EXIT

            _bazel() {
                if ! USER="nobody" "$BAZEL" \\
                        --install_base="$INSTALL_BASE" \\
                        --output_user_root="$OUTPUT_USER_ROOT" \\
                        --output_base="$OUTPUT_BASE" \\
                        "$@" \\
                        &>"$WORKDIR/bazel.log" ; then
                    cat "$WORKDIR/bazel.log" >&2
                    exit 1
                fi
            }

            mkdir workspace
            cd workspace
            touch MODULE.bazel BUILD
            echo >> .bazelrc "common --repository_cache=$REPOSITORY_CACHE"
            echo >> .bazelrc "common --noexperimental_convenience_symlinks"

            if [ "$WARM_REPOSITORY_CACHE" = "0" ]; then
                # Run bazel with no args; this should install it to the install_base.
                _bazel
            else
                # Download dependencies of commonly used rules in tests.
                echo >>BUILD 'genrule(name = "genrule", outs = ["genrule.out"], cmd = "touch $@")'
                echo >>BUILD 'sh_binary(name = "sh_binary", srcs = ["empty.sh"])'
                echo >>BUILD 'sh_test(name = "sh_test", srcs = ["empty.sh"])'
                touch empty.sh && chmod +x empty.sh
                _bazel fetch //...
                # Copy lockfile; needed to avoid registry requests.
                cp MODULE.bazel.lock "$OUT_DIR/MODULE.bazel.lock"
            fi
        """,
        env = {
            "BAZEL": ctx.executable.bazel.path,
            "OUT_DIR": out_dir.path,
            "WARM_REPOSITORY_CACHE": "1" if ctx.attr.warm_repository_cache else "0",
        },
    )
    return [DefaultInfo(files = depset([out_dir]))]

extract_bazel_installation = rule(
    implementation = _extract_bazel_installation_impl,
    attrs = {
        "bazel": attr.label(executable = True, cfg = "exec", mandatory = True, allow_single_file = True),
        "out_dir": attr.string(mandatory = True),
        "warm_repository_cache": attr.bool(default = False),
    },
    doc = "Pre-extract a bazel installation and optionally a repository cache to the given output directory.",
)

def bazel_pkg_tar(name, versions = [], **kwargs):
    """Create a tar file containing Bazel executable for each version in versions."""
    for version in versions:
        native.genrule(
            name = "bazel-{}_crossplatform".format(version),
            srcs = select({
                "//platforms/configs:linux_x86_64": ["@io_bazel_bazel-{}-linux-x86_64//file:downloaded".format(version)],
                "//platforms/configs:linux_arm64": ["@io_bazel_bazel-{}-linux-arm64//file:downloaded".format(version)],
                "//platforms/configs:macos_x86_64": ["@io_bazel_bazel-{}-darwin-x86_64//file:downloaded".format(version)],
                "//platforms/configs:macos_arm64": ["@io_bazel_bazel-{}-darwin-arm64//file:downloaded".format(version)],
            }),
            outs = ["bazel-{}".format(version)],
            cmd_bash = "cp $(SRCS) $@",
            executable = True,
            **kwargs
        )

        # Pre-warm repository cache only for bazel 8+, where we've started
        # using repository_cache combined with MODULE.bazel.lock to make
        # bazel work without network access.
        warm_repository_cache = int(version.split(".")[0]) >= 8
        extract_bazel_installation(
            name = "bazel-{}_extract_installation".format(version),
            bazel = ":bazel-{}_crossplatform".format(version),
            out_dir = "bazel-{}_outdir".format(version),
            warm_repository_cache = warm_repository_cache,
            # If we are warming the repository cache then we need network access
            # so that this rule can download the dependencies. Targets that use
            # this rule can then use the cached dependencies without needing the
            # network.
            exec_properties = {"dockerNetwork": "bridge"} if warm_repository_cache else {},
            **kwargs
        )
    pkg_tar(
        name = name,
        srcs = [":bazel-{}_crossplatform".format(version) for version in versions],
        package_dir = "/bazel",
        **kwargs
    )
