load("//rules/typescript:index.bzl", "ts_library")
load(":defs.bzl", "TERMINAL_V2_ENABLED")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

# TODO(bduffany): Once we are on terminal v2, remove all these
# targets except for terminal2, and rename terminal2 to "terminal".
genrule(
    name = "index",
    outs = ["terminal.tsx"],
    cmd_bash = """
        echo '
import { TerminalComponent } from "./%s";

export { TerminalComponent };
export default TerminalComponent;' > $@
    """ % ("terminal2" if TERMINAL_V2_ENABLED else "terminal1"),
)

ts_library(
    name = "terminal",
    srcs = ["terminal.tsx"],
    deps = [":terminal2"] if TERMINAL_V2_ENABLED else [":terminal1"],
)

ts_library(
    name = "terminal1",
    srcs = ["terminal1.tsx"],
    deps = [
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//proto:eventlog_ts_proto",
        "@npm//@types/react",
        "@npm//@types/react-lazylog",
        "@npm//@types/react-virtualized",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//react-lazylog",
        "@npm//react-virtualized",
    ],
)

ts_library(
    name = "terminal2",
    srcs = glob(
        ["*.tsx"],
        exclude = ["terminal1.tsx"],
    ),
    deps = [
        "//app/components/input",
        "//app/components/spinner",
        "//app/errors",
        "//app/service",
        "//app/util:math",
        "//app/util:scroller",
        "//proto:eventlog_ts_proto",
        "@npm//@types/react",
        "@npm//@types/react-virtualized-auto-sizer",
        "@npm//@types/react-window",
        "@npm//lucide-react",
        "@npm//memoize-one",
        "@npm//react",
        "@npm//react-virtualized-auto-sizer",
        "@npm//react-window",
    ],
)
