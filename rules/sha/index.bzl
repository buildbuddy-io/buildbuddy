# Calculates the sha1 of the contents of the files in `srcs` and outputs it to a file called `name`.sum.
def sha(name, srcs, **kwargs):
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [name + ".sum"],
        cmd_bash = """
        # Replaces host config paths like "bazel-out/{k8-opt,k8-fastbuild,k8-opt-ST-abc123}" etc.
        # with just "bazel-out/CONFIG"
        normalize_config_paths() {
            perl -p -e 's@ bazel-out/.*?/@ bazel-out/CONFIG/@'
        }
        find $(SRCS) -type f | sort | xargs shasum | normalize_config_paths | shasum | awk '{ print $$1 }' > $@
        """,
        local = 1,
        **kwargs
    )
