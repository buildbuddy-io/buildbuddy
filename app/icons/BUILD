load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "file_icon",
    srcs = ["file_icon.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//app/util:file_types",
    ],
)

ts_library(
    name = "github",
    srcs = ["github.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "github_lucide",
    srcs = ["github_lucide.tsx"],
    deps = ["//:node_modules/lucide-react"],
)

ts_library(
    name = "google",
    srcs = ["google.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)
