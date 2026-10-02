load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

ts_library(
    name = "badges",
    srcs = ["badges.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//enterprise/atlas/app/lib:format",
        "//proto:atlas_ts_proto",
    ],
)

ts_library(
    name = "cluster_picker",
    srcs = ["cluster_picker.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//enterprise/atlas/app/lib:config",
        "//enterprise/atlas/app/lib:router",
    ],
)

ts_library(
    name = "copy_button",
    srcs = ["copy_button.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//app/util:clipboard",
    ],
)

ts_library(
    name = "entry_row",
    srcs = ["entry_row.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//enterprise/atlas/app/components:badges",
        "//enterprise/atlas/app/components:copy_button",
        "//enterprise/atlas/app/components:link",
        "//enterprise/atlas/app/lib:format",
        "//enterprise/atlas/app/lib:router",
        "//proto:atlas_ts_proto",
    ],
)

ts_library(
    name = "link",
    srcs = ["link.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//enterprise/atlas/app/lib:router",
    ],
)

ts_library(
    name = "theme_toggle",
    srcs = ["theme_toggle.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/tslib",
        "//enterprise/atlas/app/lib:theme",
    ],
)
