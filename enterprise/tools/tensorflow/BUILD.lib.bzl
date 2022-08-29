package(default_visibility = ["//visibility:public"])

cc_library(
    name = "libtensorflow",
    srcs = glob(["tensorflow/**/*.h"]) + [
        "@libtensorflow-cpu-linux-x86_64-${TENSORFLOW_VERSION}//:libtensorflow",
    ],
)

cc_library(
    name = "libstdc++",
    srcs = ["libstdc++.so.6"],
)

cc_library(
    name = "libgcc_s",
    srcs = ["libgcc_s.so.1"],
)
