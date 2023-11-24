DEFAULT_CMD_TPL = """
export ROOTDIR=$$(pwd) &&
export PACKAGEDIR=$$(dirname $(location {package})) &&
export PATH=$$ROOTDIR/$$(dirname $(location {yarn})):$$ROOTDIR/$$(dirname $(location {node})):$$PATH &&
cd $$PACKAGEDIR &&
yarn install &&
yarn {command} &&
cd build &&
tar -cvf ../build.tar * &&
cd $$ROOTDIR &&
mv $$PACKAGEDIR/build.tar $@
"""

EXECUTABLE_CMD_TPL = """
cat << EOF > $@
export PATH=$$(pwd)/$$(dirname $(location {yarn})):$$(pwd)/$$(dirname $(location {node})):$$PATH &&
cd $$(dirname $(location {package})) &&
yarn install &&
yarn {command}
EOF
"""

def yarn(name, srcs, package, command = "build", deps = [], yarn = "@yarn//:yarn_bin", node = "@nodejs_host//:node_bin", **kwargs):
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
        node = node,
        command = command,
    )

    native.genrule(
        name = name,
        srcs = srcs + [package] + deps,
        outs = [name + extension],
        cmd_bash = cmd,
        executable = executable,
        tools = [yarn, node],
        local = 1,
        **kwargs
    )
