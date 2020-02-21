load("@k8s_deploy//:defaults.bzl", "k8s_deploy")
load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")
load("@io_bazel_rules_k8s//k8s:objects.bzl", "k8s_objects")

# TODO(tylerw): break depoloyment.yaml apart.
# TODO(tylerw): do the etsy context thing to set dev/prod vars.
k8s_object(
    name = "buildbuddy_dev_app",
    cluster = "gke_flame-build_us-west1-a_buildbuddy-dev",
    images = {
        "gcr.io/flame-build/buildbuddy-app-dev:server_image": "//server:buildbuddy_image",
    },
    template = ":buildbuddy-app.dev.yaml",
)

k8s_objects(
    name = "dev",
    objects = [
        ":buildbuddy_dev_app",
    ],
)
