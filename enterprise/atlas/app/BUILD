load("@aspect_rules_esbuild//esbuild:defs.bzl", "esbuild")
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//rules/sha:index.bzl", "sha")
load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

esbuild(
    name = "app_bundle",
    # Causes splitting to be very slow and fail on unused references to "crypto" and "fs".
    # https://github.com/aspect-build/rules_esbuild/issues/190
    bazel_sandbox_plugin = False,
    config = {
        "resolveExtensions": [
            ".mjs",
            ".js",
        ],
    },
    define = {"process.env.NODE_ENV": '"production"'},
    entry_points = ["app.tsx"],
    metafile = False,
    minify = select({
        "//:fastbuild": False,
        "//conditions:default": True,
    }),
    deps = [":app"],
)

ts_library(
    name = "app",
    srcs = ["app.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/lucide-react",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//enterprise/atlas/app/root",
    ],
)

genrule(
    name = "style",
    srcs = [
        "//app/root:colors.css",
        "//app/icons:icons.css",
        "//app/components/button:button.css",
        "//app/components/input:input.css",
        "//app/components/select:select.css",
        "//app/components/checkbox:checkbox.css",
        "//app/components/spinner:spinner.css",
        "atlas.css",
    ],
    outs = ["style.css"],
    cmd_bash = """
        touch out;
        for f in $(SRCS); do
            if [[ ! -d $$f ]]; then
            (cat $$f; echo) >> out;
            fi;
        done;
        cat out > $@;
    """,
)

sha(
    name = "sha",
    srcs = [
        ":app_bundle",
        ":style.css",
    ],
)

genrule(
    name = "empty_for_embedsrcs",
    outs = ["empty"],
    cmd_bash = "touch $@",
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = select({
        "//:fastbuild": [
            # Serve the app from bazel runfiles when developing locally.
            ":empty",  # embed fails with no embedsrcs
        ],
        "//conditions:default": [
            ":app_bundle",
            ":style",
            ":sha",
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/app",
    deps = ["//server/util/fileresolver"],
)
