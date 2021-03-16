def prettier(name, args):
    native.sh_binary(
        name = name,
        srcs = ["prettier.sh"],
        args = [
            "$(location @nodejs//:node)",
            "$(location @npm//:node_modules/prettier/bin-prettier.js)",
        ] + args,
        data = [
            "@nodejs//:node",
            "@npm//:node_modules/prettier/bin-prettier.js",
        ],
    )
