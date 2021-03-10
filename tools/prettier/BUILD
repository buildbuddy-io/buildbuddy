package(default_visibility = ["//visibility:public"])

sh_binary(
    name = "check",
    srcs = ["prettier.sh"],
    args = ["--check"],
    data = ["@npm//prettier"],
)

sh_binary(
    name = "fix",
    srcs = ["prettier.sh"],
    args = ["--write"],
    data = ["@npm//prettier"],
)
