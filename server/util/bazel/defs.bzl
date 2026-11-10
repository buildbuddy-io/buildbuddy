"""Rules for packaging Bazel binaries and extracted installations."""

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def _extract_bazel_installation_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.attr.out_dir)
    ctx.actions.run_shell(
        outputs = [out_dir],
        inputs = [ctx.executable.bazel],
        mnemonic = "ExtractBazelInstall",
        command = """
            set -e
            # Actions don't get a PATH by default. Export bash's default PATH
            # so that bazel's repository rules can find host tools.
            export PATH
            # Bazel 7's native sh_binary / sh_test depend on the autodetected
            # C++ toolchain, and autodetection fails if there is no C compiler
            # in the exec image. Nothing here compiles C++, so point
            # autodetection at a no-op compiler in that case.
            if [ -z "${CC:-}" ] && ! command -v gcc >/dev/null; then
                export CC=/bin/true
            fi

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
                # For Bazel 9+, sh_binary and sh_test are no longer native rules;
                # they must be loaded from @rules_shell.
                if [ "$USE_RULES_SHELL" = "1" ]; then
                    echo >>MODULE.bazel 'bazel_dep(name = "rules_shell", version = "0.6.1")'
                    echo >>BUILD 'load("@rules_shell//shell:sh_binary.bzl", "sh_binary")'
                    echo >>BUILD 'load("@rules_shell//shell:sh_test.bzl", "sh_test")'
                fi
                echo >>BUILD 'genrule(name = "genrule", outs = ["genrule.out"], cmd = "touch $@")'
                echo >>BUILD 'sh_binary(name = "sh_binary", srcs = ["empty.sh"])'
                echo >>BUILD 'sh_test(name = "sh_test", srcs = ["empty.sh"])'
                touch empty.sh && chmod +x empty.sh
                _bazel fetch //...
                # Copy lockfile; needed to avoid registry requests.
                cp MODULE.bazel.lock "$OUT_DIR/MODULE.bazel.lock"

                if [ "$USE_VENDOR_DIR" = "1" ]; then
                    # `bazel vendor` writes all bzlmod-resolved module
                    # sources into a platform-neutral directory. Test
                    # invocations can then use --vendor_dir=... with
                    # --lockfile_mode=error and make no network requests,
                    # regardless of host OS/arch. This is more robust than
                    # relying on a repository_cache populated on one exec
                    # platform and consumed on another.
                    VENDOR_DIR="$OUT_DIR/vendor_dir"
                    _bazel vendor --vendor_dir="$VENDOR_DIR" //...
                    # `bazel vendor` leaves behind a convenience
                    # symlink (bazel-external -> output_base/external)
                    # that points into our scratch WORKDIR. It has no
                    # relationship to the vendored sources and becomes a
                    # dangling symlink once WORKDIR is cleaned up, which
                    # makes Bazel reject the tree artifact. Remove it.
                    rm -f "$VENDOR_DIR/bazel-external"
                    # `bazel mod vendor` marks the vendor dir with a
                    # VENDOR.bazel file listing pinned repo names. Strip
                    # absolute paths from any generated files so the
                    # artifact is reproducible and relocatable.
                    if [ -f "$VENDOR_DIR/VENDOR.bazel" ]; then
                        # VENDOR.bazel contains only repo-name entries; no
                        # paths. Nothing to rewrite today, but assert it so
                        # we notice if Bazel ever starts embedding paths.
                        if grep -qE "$WORKDIR|$OUT_DIR" "$VENDOR_DIR/VENDOR.bazel"; then
                            echo >&2 "VENDOR.bazel contains absolute paths; vendor dir is not relocatable"
                            cat >&2 "$VENDOR_DIR/VENDOR.bazel"
                            exit 1
                        fi
                    fi
                fi
            fi
        """,
        env = {
            "BAZEL": ctx.executable.bazel.path,
            "OUT_DIR": out_dir.path,
            "WARM_REPOSITORY_CACHE": "1" if ctx.attr.warm_repository_cache else "0",
            "USE_RULES_SHELL": "1" if ctx.attr.use_rules_shell else "0",
            "USE_VENDOR_DIR": "1" if ctx.attr.use_vendor_dir else "0",
        },
    )
    return [DefaultInfo(files = depset([out_dir]))]

extract_bazel_installation = rule(
    implementation = _extract_bazel_installation_impl,
    attrs = {
        "bazel": attr.label(executable = True, cfg = "exec", mandatory = True, allow_single_file = True),
        "out_dir": attr.string(mandatory = True),
        "warm_repository_cache": attr.bool(default = False),
        "use_rules_shell": attr.bool(default = False),
        "use_vendor_dir": attr.bool(
            default = False,
            doc = "If set, also run `bazel vendor` and write the " +
                  "vendor dir to `$OUT_DIR/vendor_dir`. Requires " +
                  "`warm_repository_cache = True` and Bazel 7.1+.",
        ),
    },
    doc = "Pre-extract a bazel installation and optionally a repository cache to the given output directory.",
)

# Bazel versions that tests using //server/testutil/testbazel run against.
# TESTBAZEL_DEFAULT_VERSION is the one the testbazel library embeds.
TESTBAZEL_DEFAULT_VERSION = "9.3.2"

# Other LTS versions. Every test wrapped with bazel_versions_test also runs
# against these.
TESTBAZEL_LTS_VERSIONS = [
    "8.8.2",
    "7.7.2",
]

# Release candidates. Their targets are tagged manual and bazel-rc so that
# they only run in the nightly "Bazel RC tests" workflow.
TESTBAZEL_RC_VERSIONS = [
    "10.0.0rc2",
    "8.9.0rc1",
]

_TESTBAZEL_PKG = "//server/testutil/testbazel"
_TESTBAZEL_IMPORTPATH = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"

def bazel_versions_test(rule, name, data = [], x_defs = {}, tags = None, **kwargs):
    """Defines a test that uses testbazel once per supported Bazel version.

    `name` runs against TESTBAZEL_DEFAULT_VERSION. Each other version gets a
    target named `<name>_bazel_<version with dots replaced by underscores>`,
    which overrides the testbazel library's x_defs to point at that version.
    Release-candidate targets are tagged manual and bazel-rc, unless the test
    is already manual.

    Args:
      rule: The test rule or macro to wrap, e.g. go_test or go_web_test_suite.
        It must forward data and x_defs to a go_test.
      name: Name of the default-version test.
      data: Runtime data for the test.
      x_defs: x_defs for the test.
      tags: Tags for the test.
      **kwargs: Additional arguments passed to every generated test.
    """
    rule(name = name, data = data, x_defs = x_defs, tags = tags, **kwargs)
    for version in TESTBAZEL_LTS_VERSIONS + TESTBAZEL_RC_VERSIONS:
        binary = "{}:bazel-{}_crossplatform".format(_TESTBAZEL_PKG, version)
        installation = "{}:bazel-{}_extract_installation".format(_TESTBAZEL_PKG, version)
        version_x_defs = dict(x_defs)
        version_x_defs.update({
            _TESTBAZEL_IMPORTPATH + ".Version": version,
            _TESTBAZEL_IMPORTPATH + ".bazelRlocationpath": "$(rlocationpath {})".format(binary),
            _TESTBAZEL_IMPORTPATH + ".outdirRlocationpath": "$(rlocationpath {})".format(installation),
        })
        version_tags = list(tags or [])
        if version in TESTBAZEL_RC_VERSIONS and "manual" not in version_tags:
            # Tests that are already manual (i.e. disabled) stay out of the
            # nightly bazel-rc run.
            version_tags += ["manual", "bazel-rc"]
        rule(
            name = "{}_bazel_{}".format(name, version.replace(".", "_")),
            data = data + [binary, installation],
            x_defs = version_x_defs,
            tags = version_tags,
            **kwargs
        )

def bazel_binary_targets(versions = [], **kwargs):
    """Creates Bazel binary and extracted-installation targets.

    Args:
      versions: Bazel versions for which to create targets.
      **kwargs: Additional arguments passed to the generated targets.
    """
    for version in versions:
        copy_file(
            name = "bazel-{}_crossplatform".format(version),
            src = select({
                "//platforms/configs:linux_x86_64": "@io_bazel_bazel-{}-linux-x86_64//file:downloaded".format(version),
                "//platforms/configs:linux_arm64": "@io_bazel_bazel-{}-linux-arm64//file:downloaded".format(version),
                "//platforms/configs:macos_x86_64": "@io_bazel_bazel-{}-darwin-x86_64//file:downloaded".format(version),
                "//platforms/configs:macos_arm64": "@io_bazel_bazel-{}-darwin-arm64//file:downloaded".format(version),
            }),
            out = "bazel-{}".format(version),
            allow_symlink = True,
            is_executable = True,
            **kwargs
        )

        # For Bazel 7+, pre-warm the repository cache, write a
        # MODULE.bazel.lock, and vendor the bzlmod module sources (including
        # @rules_shell) so that tests can run fully offline with
        # --lockfile_mode=error and --vendor_dir on any host OS/arch. Bazel 6
        # predates `bazel vendor` and bzlmod-by-default, so it only gets an
        # extracted install_base.
        #
        # @rules_shell is strictly required on Bazel 9+ (sh_binary / sh_test
        # are no longer native). On older versions it's optional but cheap,
        # and lets test-written BUILD files use a single
        # `load("@rules_shell//shell:...")` form that works on every version.
        major_version = int(version.split(".")[0])
        warm_repository_cache = major_version >= 7
        use_rules_shell = major_version >= 7
        use_vendor_dir = major_version >= 7
        extract_bazel_installation(
            name = "bazel-{}_extract_installation".format(version),
            bazel = ":bazel-{}_crossplatform".format(version),
            out_dir = "bazel-{}_outdir".format(version),
            warm_repository_cache = warm_repository_cache,
            use_rules_shell = use_rules_shell,
            use_vendor_dir = use_vendor_dir,
            # If we are warming the repository cache then we need network access
            # so that this rule can download the dependencies. Targets that use
            # this rule can then use the cached dependencies without needing the
            # network.
            exec_properties = {"dockerNetwork": "bridge"} if warm_repository_cache else {},
            **kwargs
        )

def bazel_pkg_tar(name, versions = [], **kwargs):
    """Create a tar file containing Bazel executables for the given versions."""
    bazel_binary_targets(versions = versions, **kwargs)
    pkg_tar(
        name = name,
        srcs = [":bazel-{}_crossplatform".format(version) for version in versions],
        package_dir = "/bazel",
        **kwargs
    )
