data "google_client_config" "default" {}

## COMMON

locals {
  zones = ["${var.region}-a", "${var.region}-b", "${var.region}-c"]
}

resource "random_password" "password" {
  length           = 8
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

## CLUSTER

provider "kubernetes" {
  host                   = "https://${module.gke.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.gke.ca_certificate)
}

module "gke" {
  source                      = "terraform-google-modules/kubernetes-engine/google"
  project_id                  = var.project_id
  name                        = "${var.cluster_name_prefix}cluster"
  regional                    = true
  region                      = var.region
  network                     = var.network
  subnetwork                  = var.subnetwork
  ip_range_pods               = var.ip_range_pods
  ip_range_services           = var.ip_range_services
  create_service_account      = false
  service_account             = var.compute_engine_service_account
  enable_binary_authorization = false
  skip_provisioners           = false
  http_load_balancing         = false
  network_policy              = false
  horizontal_pod_autoscaling  = true
  filestore_csi_driver        = false

  node_pools = [
    {
      name               = "default-node-pool"
      machine_type       = "n2-highmem-16"
      min_count          = 1
      max_count          = 100
      local_ssd_count    = 0
      image_type         = "COS_CONTAINERD"
      auto_repair        = true
      auto_upgrade       = true
      preemptible        = false
      initial_node_count = 1
    },
    {
      name               = "executor-pool"
      machine_type       = "c2-standard-8"
      min_count          = 1
      max_count          = 100
      local_ssd_count    = 4
      disk_size_gb       = 100
      image_type         = "UBUNTU_CONTAINERD"
      auto_repair        = true
      auto_upgrade       = true
      preemptible        = true
      initial_node_count = 1
    },
  ]

  node_pools_taints = {
    all = []

    executor-pool = [
      {
        key    = "workload"
        value  = "executor"
        effect = "NO_SCHEDULE"
      },
    ]
  }
}

## NETWORK

resource "google_compute_network" "private_network" {
  project = var.project_id
  name    = "private-network"
}

resource "google_compute_global_address" "private_ip_address" {
  project       = var.project_id
  name          = "private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.private_network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.private_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

## SQL

resource "google_sql_database_instance" "main" {
  name                = var.mysql_ha_name
  project             = var.project_id
  database_version    = "MYSQL_8_0"
  region              = var.region
  deletion_protection = false

  depends_on = [google_service_networking_connection.private_vpc_connection]

  settings {
    tier              = "db-n1-standard-16"
    availability_type = "REGIONAL"
    disk_type         = "PD_SSD"
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.private_network.id
    }
    maintenance_window {
      day          = 7
      hour         = 12
      update_track = "stable"
    }
    database_flags {
      name  = "performance_schema"
      value = "on"
    }
    backup_configuration {
      enabled                        = true
      binary_log_enabled             = true
      start_time                     = "20:55"
      location                       = null
      transaction_log_retention_days = null
      backup_retention_settings {
        retained_backups = 10
        retention_unit   = "COUNT"
      }
    }
  }
}

resource "google_sql_database" "buildbuddy" {
  instance  = google_sql_database_instance.main.name
  project   = var.project_id
  name      = "buildbuddy_${var.env}"
  charset   = "utf8mb4"
  collation = "utf8mb4_general_ci"
}

resource "google_sql_user" "users" {
  instance = google_sql_database_instance.main.name
  project  = var.project_id
  name     = "buildbuddy-${var.env}"
  password = random_password.password.result
}

## BUCKETS

resource "google_storage_bucket" "static" {
  name          = "buildbuddy-${var.env}-static"
  project       = var.project_id
  location      = "US"
  force_destroy = true

  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", ]
    response_header = ["Content-Type"]
    max_age_seconds = 3600
  }
}

resource "google_storage_bucket_iam_member" "member" {
  bucket = "buildbuddy-${var.env}-static"
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

resource "google_storage_bucket" "blobs" {
  name          = "buildbuddy-${var.env}-blobs"
  project       = var.project_id
  location      = "US"
  force_destroy = true
}