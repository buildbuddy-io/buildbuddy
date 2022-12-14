def yarn(name, srcs, package, command = "build", deps = [], yarn = "@yarn//:yarn_bin", node = "@nodejs_host//:node_bin", **kwargs):
    extension = ".tar"
    executable = False
    if command != "build":
        extension = ".sh"
        executable = True

    cmd = """
        export ROOTDIR=$$(pwd) && 
        export PACKAGEDIR=$$(dirname $(location %s)) && 
        export PATH=$$ROOTDIR/$$(dirname $(location %s)):$$ROOTDIR/$$(dirname $(location %s)):$$PATH && 
        cd $$PACKAGEDIR && 
        yarn install && 
        yarn %s && 
        cd build && 
        tar -cvf ../build.tar * && 
        cd $$ROOTDIR && 
        mv $$PACKAGEDIR/build.tar $@
    """ % (package, yarn, node, command)

    if executable:
        cmd = "echo \"cd $$(dirname $(location %s)) && yarn install && yarn %s\" > $@" % (package, command)

    native.genrule(
        name = name,
        srcs = srcs + [package] + deps,
        outs = [name + extension],
        cmd_bash = cmd,
        executable = executable,
        exec_tools = [yarn, node],
        local = 1,
        **kwargs
    )
