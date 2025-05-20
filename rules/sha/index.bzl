# Calculates the sha1 of the contents of the files in `srcs` and outputs it to a file called `name`.sum.
# Also produces a file called `name`.list.sum which contains intermediate checksums for each file.
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
        _debug() {
            if [ "$${BB_RULES_SHA_DEBUG:-0}" -eq 1 ]; then
                tee /dev/stderr
                echo >&2 "rules/sha: BB_RULES_SHA_DEBUG=1; exiting (see file hashes above)"
                exit 1
            fi
            cat
        }
        find -L $(SRCS) -type f |
            sort |
            xargs shasum |
            normalize_config_paths |
            _debug |
            shasum |
            awk '{ print $$1 }' > $@
        """,
        **kwargs
    )
