# Calculates the sha1 of the contents of the files in `srcs` and outputs it to a file called `name`.sum.
def sha(name, srcs, **kwargs):
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [name + ".sum"],
        cmd = "find $(SRCS) -type f | sort | xargs shasum | shasum | awk '{ print $$1 }' > $@",
        local = 1,
        **kwargs
    )
