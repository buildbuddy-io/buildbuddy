cc_library(
    name = "libtensorflow",
    srcs = glob(["tensorflow/**/*.h"]) + [
        "@libtensorflow-cpu-linux-x86_64-${TENSORFLOW_VERSION}//:libtensorflow",
    ],
    visibility = ["//visibility:public"],
)
