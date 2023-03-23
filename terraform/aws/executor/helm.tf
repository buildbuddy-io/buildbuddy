provider "helm" {
  kubernetes {
    host                   = var.cluster_endpoint
    cluster_ca_certificate = base64decode(var.cluster_ca_certificate)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      args        = ["eks", "get-token", "--cluster-name", var.cluster_name]
      command     = "aws"
    }
  }
}

resource "helm_release" "executor" {
  name       = "executor"
  repository = "https://helm.buildbuddy.io"
  chart      = "buildbuddy-executor"

  values = [
    file("${path.module}/executor-podman-values.yaml")
  ]

  set {
    name = "config.executor.api_key"
    value = var.buildbuddy_api_key
  }

  set {
    name = "config.executor.app_target"
    value = var.buildbuddy_app_target
  }

  set {
    name = "config.executor.pool"
    value = var.buildbuddy_pool
  }
}
