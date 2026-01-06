load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "css_colors_test",
    srcs = ["css_colors_test.py"],
    data = [
        "//app/root:colors.css",
        "//enterprise/app:style",
    ],
)
