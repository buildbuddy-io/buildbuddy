variable "project_id" {
  description = "The project ID to host the cluster in"
}

variable "cluster_name_prefix" {
  description = "A prefix to append to the default cluster name"
  default     = "buildbuddy"
}

variable "region" {
  description = "The region to host the cluster in"
  default     = "us-west1"
}

variable "network" {
  description = "The VPC network to host the cluster in"
}

variable "subnetwork" {
  description = "The subnetwork to host the cluster in"
}

variable "ip_range_pods" {
  description = "The secondary ip range to use for pods"
}

variable "ip_range_services" {
  description = "The secondary ip range to use for services"
}

variable "compute_engine_service_account" {
  description = "The service account to use"
}

variable "mysql_ha_name" {
  type        = string
  description = "The name for the Cloud SQL instance"
  default     = "mysql-ha"
}

variable "mysql_ha_external_ip_range" {
  type        = string
  description = "The ip range to allow connecting from/to Cloud SQL"
  default     = "192.10.10.10/32"
}

variable "env" {
  type        = string
  description = "The environment to use: 'dev' or 'prod'"
  default     = "dev"
}