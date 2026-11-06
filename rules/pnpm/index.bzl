"""Rule for building pnpm packages."""

DEFAULT_CMD_TPL = """
# NOTE: BazelBinResolverPlugin in docusaurus.config.js depends on ROOTDIR being set
# to the original execution working directory.
export BAZEL_BINDIR=. &&
export ROOTDIR=$$(pwd) &&
export PACKAGEDIR=$$(dirname $(location {package})) &&
export PATH=$$ROOTDIR/$$(dirname $(location {pnpm})):$$ROOTDIR/$$(dirname $(NODE_PATH)):$$PATH &&
cd $$PACKAGEDIR &&
pnpm install --config.confirmModulesPurge=false &&
pnpm {command} &&
cd build &&
tar -cvf ../build.tar * &&
cd $$ROOTDIR &&
mv $$PACKAGEDIR/build.tar $@
"""

EXECUTABLE_CMD_TPL = (
    """
cat << EOF > $@
export BAZEL_BINDIR=. &&
export PATH=$$(pwd)/$$(dirname $(location {pnpm})):$$(pwd)/$$(dirname $(NODE_PATH)):$$PATH &&
cd $$(dirname $(location {package})) &&
pnpm install --config.confirmModulesPurge=false &&""" +

    # To explain the complicated escaping here:
    # starlark resolves `\\` to a literal backslash, giving us `\$$@`
    #
    # genrule interprets `\` as a literal backslash, then resolves `$$` to a
    # literal dollar sign, giving us `\$@`
    #
    # the bash process that runs the `cat` command then resolves `\$` to a
    # literal dollar sign, giving us `$@`
    #
    # the bash process that runs the pnpm command then resolves `$@` to the
    # command line arguments, effectively forwarding them to pnpm.
    """
pnpm {command} \\$$@
EOF
"""
)

def pnpm(name, srcs, package, command = "build", deps = [], pnpm = Label("@pnpm//:pnpm"), node = Label("@nodejs_toolchains//:resolved_toolchain"), **kwargs):
    """Builds a pnpm package as an archive or executable script.

    Args:
      name: Name of the generated target.
      srcs: Source files needed by the package.
      package: Label of the package.json file.
      command: pnpm command to run.
      deps: Additional package inputs.
      pnpm: Label of the pnpm executable.
      node: Label of the Node.js toolchain.
      **kwargs: Additional arguments passed to the generated genrule.
    """
    extension = ".tar"
    executable = False
    if command != "build":
        extension = ".sh"
        executable = True

    if executable:
        cmd_tpl = EXECUTABLE_CMD_TPL
    else:
        cmd_tpl = DEFAULT_CMD_TPL

    cmd = cmd_tpl.format(
        package = package,
        pnpm = pnpm,
        command = command,
    )

    native.genrule(
        name = name,
        srcs = srcs + [package] + deps,
        outs = [name + extension],
        cmd_bash = cmd,
        executable = executable,
        tools = [pnpm, node],
        toolchains = [node],
        local = 1,
        **kwargs
    )
