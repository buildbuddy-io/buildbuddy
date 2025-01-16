sh_binary(
    name = "checkstyle",
    srcs = ["checkstyle.sh"],
    data = [
        "//:go",
        "//:gofmt",
        "//:goimports.sh",
        "//cli/cmd/bb",
        "//tools/clang-format",
        "@npm//prettier/bin:prettier",
    ],
)
