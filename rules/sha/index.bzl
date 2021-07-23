def sha(name, srcs, **kwargs):
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [name + ".txt"],
        cmd = "echo \"$$(echo \"$(SRCS)\" | xargs find | sort | xargs shasum | shasum | awk '{ print $$1 }')\" > $@",
        local = 1,
        **kwargs
    )
