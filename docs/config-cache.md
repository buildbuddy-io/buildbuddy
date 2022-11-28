---
id: config-cache
title: Cache Configuration
sidebar_label: Cache
---

## Section

`cache:` The cache section enables the BuildBuddy cache and configures how and where it will store data. **Optional**

## Options

**Optional**

- `max_size_bytes:` How big to allow the cache to be (in bytes).

- `in_memory:` Whether or not to use the in_memory cache.

- `zstd_transcoding_enabled`: Whether or not to enable cache compression capabilities. You need to use `--experimental_remote_cache_compression` to activate it on your build.

- `disk:` The Disk section configures a disk-based cache.

  - `root_directory` The root directory to store cache data in, if using the disk cache. This directory must be readable and writable by the BuildBuddy process. The directory will be created if it does not exist.

**Enterprise only**

- `redis_target`: A redis target for improved RBE performance.

- `gcs:` The GCS section configures Google Cloud Storage based blob storage.

  - `bucket` The name of the GCS bucket to store files in. Will be created if it does not already exist.

  - `credentials_file` A path to a [JSON credentials file](https://cloud.google.com/docs/authentication/getting-started) that will be used to authenticate to GCS.

  - `project_id` The Google Cloud project ID of the project owning the above credentials and GCS bucket.

  - `ttl_days` The period after which cache files should be TTLd. Disabled if 0.

- `s3:` The AWS section configures AWS S3 storage.

  - `region` The AWS region

  - `bucket` The AWS S3 bucket (will be created automatically)

  - `credentials_profile` If a profile other than default is chosen, use that one.

  - `ttl_days` The period after which cache files should be TTLd. Disabled if 0.

  - By default, the S3 blobstore will rely on environment variables, shared credentials, or IAM roles. See [AWS Go SDK docs](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials) for more information.

## Example section

### Disk

```
cache:
  max_size_bytes: 10000000000  # 10 GB
  disk:
    root_directory: /tmp/buildbuddy-cache
```

### GCS & Redis (Enterprise only)

```
cache:
  redis_target: "my-redis.local:6379"
  gcs:
    bucket: "buildbuddy_blobs"
    project_id: "my-cool-project"
    credentials_file: "enterprise/config/my-cool-project-7a9d15f66e69.json"
    ttl_days: 30
```

### S3 (Enterprise only)

```
cache:
  s3:
    # required
    region: "us-west-2"
    bucket: "buildbuddy-bucket"
    # optional
    credentials_profile: "other-profile"
    ttl_days: 30
```

### Minio (Enterprise only)

```
cache:
  s3:
    static_credentials_id: "YOUR_MINIO_ACCESS_KEY"
    static_credentials_secret: "YOUR_MINIO_SECRET"
    endpoint: "http://localhost:9000"
    disable_ssl: true
    s3_force_path_style: true
    region: "us-east-1"
    bucket: "buildbuddy-cache-bucket"
```
