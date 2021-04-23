package(default_visibility = ["//visibility:public"])

[
    genrule(
        name = name,
        srcs = ["//enterprise/app:%s" % fname],
        outs = [fname],
        cmd_bash = "cp $(SRCS) $@",
    )
    for (name, fname) in [
        ("copy_app_bundle", "app_bundle.js"),
        ("copy_app_bundle_sourcemap", "app_bundle.js.map"),
        ("copy_style", "style.css"),
    ]
]

filegroup(
    name = "app_bundle",
    srcs = [
        "app_bundle.js",
        "app_bundle.js.map",
    ],
)

filegroup(
    name = "style",
    srcs = ["style.css"],
)
