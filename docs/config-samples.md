---
id: config-samples
title: Sample Configuration Files
sidebar_label: Samples
---

### Running locally (disk only)

```
app:
  build_buddy_url: "http://localhost:8080"
database:
  data_source: "sqlite3:///tmp/buildbuddy.db"
storage:
  ttl_seconds: 86400  # One day in seconds.
  chunk_file_size_bytes: 3000000  # 3 MB
  disk:
    root_directory: /tmp/buildbuddy
cache:
  max_size_bytes: 10000000000  # 10 GB
  disk:
    root_directory: /tmp/buildbuddy-cache
```

### Running with MySQL and in-memory cache

```
app:
  build_buddy_url: "http://acme.corp"
database:
  data_source: "mysql://buildbuddy_user:pAsSwOrD@tcp(12.34.56.78)/buildbuddy_db"
storage:
  ttl_seconds: 86400  # One day in seconds.
  chunk_file_size_bytes: 3000000  # 3 MB
  disk:
    root_directory: /data/buildbuddy
cache:
  max_size_bytes: 10000000000  # 10 GB
  in_memory: true
```

## Enterprise

### Running with your own auth provider

```
app:
  build_buddy_url: "http://acme.corp"
database:
  data_source: "mysql://buildbuddy_user:pAsSwOrD@tcp(12.34.56.78)/buildbuddy_db"
storage:
  ttl_seconds: 86400  # One day in seconds.
  chunk_file_size_bytes: 3000000  # 3 MB
  disk:
    root_directory: /data/buildbuddy
cache:
  max_size_bytes: 10000000000  # 10 GB
  in_memory: true
auth:
  oauth_providers:
    - issuer_url: "https://accounts.google.com"
      client_id: "12345678911-f1r0phjnhbabcdefm32etnia21keeg31.apps.googleusercontent.com"
      client_secret: "sEcRetKeYgOeShErE"
```

### Fully loaded

```
app:
  build_buddy_url: "https://app.buildbuddy.mydomain"
  events_api_url: "grpcs://events.buildbuddy.mydomain:1986"
  cache_api_url: "grpcs://cache.buildbuddy.mydomain:1986"
database:
  data_source: "mysql://user:pass@tcp(12.34.56.78)/database_name"
storage:
  ttl_seconds: 2592000  # 30 days
  chunk_file_size_bytes: 3000000  # 3 MB
  gcs:
    bucket: "buildbuddy_prod_blobs"
    project_id: "flame-build"
    credentials_file: "your_service-acct.json"
cache:
    redis_target: "12.34.56.79:6379"
    gcs:
      bucket: "buildbuddy_cache"
      project_id: "your_gcs_project_id"
      credentials_file: "/path/to/your/credential/file.json"
      ttl_days: 30
auth:
  oauth_providers:
    - issuer_url: "https://your-custom-domain.okta.com"
      client_id: "0aaa5twc7sx0kUW123x6"
      client_secret: "P8fRAYxWMmGhdA9040GV2_q9MZ6esTJif1n4BubxU"
ssl:
  enable_ssl: true
  client_ca_cert_file: your_ca.crt
  client_ca_key_file: your_ca.pem
remote_execution:
  enable_remote_exec: true
```
