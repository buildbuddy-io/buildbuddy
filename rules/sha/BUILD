load("@rules_shell//shell:sh_test.bzl", "sh_test")
load(":index.bzl", "sha")

sh_test(
    name = "sha_test",
    srcs = ["sha_test.sh"],
    data = [
        ":testdata_sha.sum",
    ],
    env = {
        "TESTDATA_SUM": "$(location :testdata_sha.sum)",
    },
)

sha(
    name = "testdata_sha",
    srcs = [
        # Include a generated file to test config dir normalization.
        "//rules/sha/testdata:generated_file.txt",
        # Include a dir symlink to test that 'find' is following symlinks.
        "//rules/sha/testdata:dir.ln",
    ],
)
