def _remote_docker_build_impl(ctx):
    archive = ctx.actions.declare_file("dockerfile_image.tar.gz")
    build_script = ctx.actions.declare_file("docker_build.sh")
    ctx.actions.expand_template(
        template = ctx.file._build_script_template,
        output = build_script,
        substitutions = {
            "{image_name}": ctx.label.package + ":" + ctx.label.name,
            "{dockerfile}": ctx.file.dockerfile.path,
            "{archive}": archive.path,
        },
        is_executable = True,
    )
    ctx.actions.run(
        inputs = [ctx.file.dockerfile],
        outputs = [archive],
        executable = build_script,
    )
    return [DefaultInfo(files = depset([archive]))]

_remote_docker_build = rule(
    implementation = _remote_docker_build_impl,
    attrs = {
        "_build_script_template": attr.label(
            default = Label("//rules/docker:docker_build.sh.tpl"),
            allow_single_file = True,
        ),
        "dockerfile": attr.label(
            allow_single_file = ["Dockerfile"],
        ),
    },
)

def remote_docker_build(name, recycling_key = "", exec_properties = {}, **kwargs):
    """ remote_docker_build rule executes a docker build in a remote docker daemon
    and output a tar.gz archive of the image.
    """
    recycling_key = recycling_key if recycling_key else name
    _remote_docker_build(
        name = name,
        exec_properties = {
            "dockerNetwork": "bridge",
            "workload-isolation-type": "firecracker",
            "init-dockerd": "true",
            "EstimatedCPU": "16",
            "EstimatedMemory": "32G",
            "EstimatedFreeDiskBytes": "50G",
            "recycle-runner": "true",
            "runner-recycling-key": "remote_docker_build_" + recycling_key,
        } | exec_properties,
        target_compatible_with = [
            # We don't depend on cc toolchain, but we use this to differentiate
            # between local and remote build.
            "@bazel_tools//tools/cpp:gcc",
        ],
        tags = ["manual", "docker"],
        **kwargs
    )
