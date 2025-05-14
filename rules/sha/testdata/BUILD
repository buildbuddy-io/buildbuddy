package(default_visibility = ["//rules/sha:__subpackages__"])

genrule(
    name = "generate_generated_file.txt",
    outs = ["generated_file.txt"],
    cmd_bash = "printf 'GENERATED_FILE_CONTENTS' > $@",
)

exports_files([
    "dir.ln",
    "dir/child.txt",
])
