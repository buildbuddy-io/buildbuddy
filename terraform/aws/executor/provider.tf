terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.21.1"
    }

    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.10.1"
    }
  }

  required_version = "~> 1.5.0"
}
