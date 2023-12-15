def _remote_kaniko_build_impl(ctx):
    build_script = ctx.actions.declare_file("kaniko_build.sh")
    archive = ctx.actions.declare_file("image.tar")
    digest_file = ctx.actions.declare_file("digest.txt")
    ctx.actions.expand_template(
        template = ctx.file._build_script_template,
        output = build_script,
        substitutions = {
            "{archive}": archive.path,
            "{context_directory_path}": ctx.label.package,
            "{digest_file}": digest_file.path,
            "{dockerfile}": ctx.file.dockerfile.path,
            "{image_name}": ctx.label.package + ":" + ctx.label.name,
            "{kaniko}": ctx.executable._kaniko.path,
            "{registry_mirror}": ctx.attr.registry_mirror,
        },
        is_executable = True,
    )

    inputs = [ctx.file.dockerfile, ctx.file._kaniko]
    for f in ctx.files.context_directory:
        inputs.append(f)

    ctx.actions.run(
        inputs = inputs,
        outputs = [archive, digest_file],
        executable = build_script,
        mnemonic = "RemoteKanikoBuild",
        progress_message = "Kaniko: building image with %{input}...",
    )
    return [
        DefaultInfo(
            files = depset([archive]),
        ),
        OutputGroupInfo(
            image_digest = depset([digest_file]),
        ),
    ]

_remote_kaniko_build = rule(
    implementation = _remote_kaniko_build_impl,
    attrs = {
        "_kaniko": attr.label(
            default = Label("@com_github_googlecontainertools_kaniko//cmd/executor"),
            allow_single_file = True,
            executable = True,
            cfg = "exec",
        ),
        "_build_script_template": attr.label(
            default = Label("//rules/kaniko:kaniko_build.sh.tpl"),
            allow_single_file = True,
        ),
        "context_directory": attr.label_list(
            allow_files = True,
        ),
        "dockerfile": attr.label(
            allow_single_file = ["Dockerfile"],
        ),
        "registry_mirror": attr.string(
            default = "mirror.gcr.io",
        ),
    },
)

def remote_kaniko_build(name, recycling_key = "", exec_properties = {}, **kwargs):
    """ remote_docker_build rule executes a kaniko build in RBE
    and output a tar.gz archive of the image.
    """
    recycling_key = recycling_key if recycling_key else name
    native.filegroup(
        name = name + "_context_directory",
        srcs = native.glob(["**"]),
    )

    # TODO(sluongng): making this compatibnle with remote execution only.
    _remote_kaniko_build(
        name = name,
        context_directory = [":" + name + "_context_directory"],
        exec_properties = {
            "dockerNetwork": "bridge",
            "EstimatedCPU": "16",
            "EstimatedMemory": "32G",
            "recycle-runner": "true",
            "runner-recycling-key": "remote_docker_build_" + recycling_key,
        } | exec_properties,
        tags = ["manual", "docker", "requires-network"],
        **kwargs
    )
