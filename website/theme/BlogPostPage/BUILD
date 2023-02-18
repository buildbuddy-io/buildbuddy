load("//rules/typescript:index.bzl", "ts_library")

ts_library(
    name = "index",
    srcs = ["index.tsx"],
    deps = [
        "//website/theme:BlogPostItem",
        "//website/theme:BlogSidebar",
        "//website/theme/BlogPostPage:styles.module.css",
        "@npm//@theme/BlogPostPage",
        "@npm//@theme/BlogPostPaginator",
        "@npm//@theme/EditThisPage",
        "@npm//@theme/Layout",
        "@npm//@types/react",
        "@npm//react",
    ],
)
