DEFAULT_CMD_TPL = """
# NOTE: BazelBinResolverPlugin in docusaurus.config.js depends on ROOTDIR being set
# to the original execution working directory.
export BAZEL_BINDIR=. &&
export ROOTDIR=$$(pwd) &&
export PACKAGEDIR=$$(dirname $(location {package})) &&
export PATH=$$ROOTDIR/$$(dirname $(location {yarn})):$$ROOTDIR/$$(dirname $(NODE_PATH)):$$PATH &&
cd $$PACKAGEDIR &&
yarn install &&
yarn {command} &&
cd build &&
tar -cvf ../build.tar * &&
cd $$ROOTDIR &&
mv $$PACKAGEDIR/build.tar $@
"""

EXECUTABLE_CMD_TPL = ("""
cat << EOF > $@
export BAZEL_BINDIR=. &&
export PATH=$$(pwd)/$$(dirname $(location {yarn})):$$(pwd)/$$(dirname $(NODE_PATH)):$$PATH &&
cd $$(dirname $(location {package})) &&
yarn install &&""" +
# To explain the complicated escaping here:
# python resolves `\\` to a literal backslash, giving us `\$$@`
#
# genrule interprets `\` as a literal backslash, then resolves `$$` to a literal
# dollar sign, giving us `\$@`
#
# the bash process that runs the `cat` command then resolves `\$` to a literal
# dollar sign, giving us `$@`
#
# the bash process that runs the yarn command then resolves `$@` to the command
# line arguments, effectively forwarding them to yarn.
"""
yarn {command} \\$$@
EOF
"""
)

def yarn(name, srcs, package, command = "build", deps = [], yarn = Label("//rules/yarn"), node = Label("@nodejs_toolchains//:resolved_toolchain"), **kwargs):
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
        yarn = yarn,
        command = command,
    )

    native.genrule(
        name = name,
        srcs = srcs + [package] + deps,
        outs = [name + extension],
        cmd_bash = cmd,
        executable = executable,
        tools = [yarn, node],
        toolchains = [node],
        local = 1,
        **kwargs
    )
