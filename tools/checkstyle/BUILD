sh_binary(
    name = "checkstyle",
    srcs = ["checkstyle.sh"],
    data = [
        "//:go",
        "//:gofmt",
        "//cli/cmd/bb",
        "//tools/clang-format",
        "@npm//prettier/bin:prettier",
    ],
)
