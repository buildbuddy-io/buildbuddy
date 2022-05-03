data "google_client_config" "default" {}

locals {
  zones = ["${var.region}-a", "${var.region}-b", "${var.region}-c"]
}

resource "random_password" "password" {
  length           = 8
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

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

resource "google_sql_database_instance" "main" {
  name                = var.mysql_ha_name
  project             = var.project_id
  database_version    = "MYSQL_8_0"
  region              = var.region
  deletion_protection = false

  settings {
    tier              = "db-n1-standard-16"
    availability_type = "REGIONAL"
    disk_type         = "PD_SSD"
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
    collation = "utf8mb4_general_ci"
  }
}


//   ip_configuration = {
//     ipv4_enabled       = true
//     require_ssl        = true
//     private_network    = null
//     allocated_ip_range = null
//     authorized_networks = [
//       {
//         name  = "${var.project_id}-cidr"
//         value = var.mysql_ha_external_ip_range
//       },
//     ]
//   }


//   db_name      = "$buildbuddy_{var.env}"
//   db_charset   = "utf8mb4"
//   db_
//   user_name     = "$buildbuddy-{var.env}"
//   user_password = random_password.password.result
// }