load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = glob(
        [
            "*.tsx",
            "*.ts",
        ],
        exclude = ["*_test.*"],
    ),
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/rxjs",
        "//:node_modules/tslib",
        "//app/components/scrollbar",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:color",
        "//app/util:dom",
        "//app/util:math",
    ],
)

ts_jasmine_node_test(
    name = "profile_model_test",
    srcs = ["profile_model_test.ts"],
    deps = [":flame_chart"],
)
