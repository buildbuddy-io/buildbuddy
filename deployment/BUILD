load("@k8s_deploy//:defaults.bzl", "k8s_deploy")
load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")
load("@io_bazel_rules_k8s//k8s:objects.bzl", "k8s_objects")
load("@io_bazel_rules_docker//container:push.bzl", "container_push")

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

# Definition for uploading new releases of the on_prem docker image.
# Running this? You may want to update the onprem image version configured in
# deployment/buildbuddy-app.onprem.yaml so on-prem users actually get your
# new version!
container_push(
    name = "release_onprem",
    format = "Docker",
    image = "//server:buildbuddy_image",

    # Any of these components may have variables. They are set by passing
    # --define version=1.2.3 as arguments to the bazel build command.
    registry = "gcr.io",
    repository = "flame-public/buildbuddy-app-onprem",
    tag = "server-image-$(version)",
)
