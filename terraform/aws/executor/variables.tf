variable "cluster_name" {
  description = "Kubernetes cluster name"
  type        = string
}

variable "cluster_endpoint" {
  description = "Kubernetes cluster endpoint"
  type        = string
}

variable "cluster_ca_certificate" {
  description = "Kubernetes cluster ca certificate"
  type        = string
}

variable "buildbuddy_api_key" {
  description = "BuildBuddy executor api key"
  type        = string
}

variable "buildbuddy_app_target" {
  description = "BuildBuddy app target"
  type        = string
  default     = "grpcs://remote.buildbuddy.io"
}

variable "buildbuddy_pool" {
  description = "BuildBuddy executor pool name"
  type        = string
  default     = "my-pool"
}
