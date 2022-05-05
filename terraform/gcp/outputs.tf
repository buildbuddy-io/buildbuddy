output "kubernetes_endpoint" {
  sensitive = true
  value     = module.gke.endpoint
}

output "client_token" {
  sensitive = true
  value     = base64encode(data.google_client_config.default.access_token)
}

output "ca_certificate" {
  value     = module.gke.ca_certificate
  sensitive = true
}

output "service_account" {
  description = "The default service account used for running nodes."
  value       = module.gke.service_account
}

output "db_name" {
  description = "Name of the buildbuddy database"
  value       = google_sql_database.buildbuddy.name
}

output "db_private_ip_address" {
  description = "The public IPv4 address of the master instance."
  value       = google_sql_database_instance.main.private_ip_address
}

output "static_bucket_name" {
  description = "The static resources bucket name"
  value       = google_storage_bucket.static.name
}

output "CACHE_GCS_BUCKET" {
  description = "The blobs bucket name"
  value       = google_storage_bucket.blobs.name
}

# TODO(tylerw): use cluster output?
output "DEV_APP_CLUSTER" {
  description = "The full name of the dev app cluster"
  value       = "gke_${var.project_id}_${var.region}_${var.cluster_name_prefix}cluster"
}

output "ENV" {
  description = "The environment to use"
  value       = var.env
}

output "LOCATION" {
  description = "The region the cluster is hosted in"
  value       = var.region
}
