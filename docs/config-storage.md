---
id: config-storage
title: Storage Configuration
sidebar_label: Storage
---

`storage:` The Storage section configures where and how BuildBuddy will store blob data. **Required**

## Options

One of the following sections is **Required**

- `disk:` The Disk section configures disk-based blob storage.

  - `root_directory` The root directory to store all blobs in, if using disk based storage. This directory must be readable and writable by the BuildBuddy process. The directory will be created if it does not exist.

- `gcs:` The GCS section configures Google Cloud Storage based blob storage.

  - `bucket` The name of the GCS bucket to store files in. Will be created if it does not already exist.

  - `credentials_file` A path to a [JSON credentials file](https://cloud.google.com/docs/authentication/getting-started) that will be used to authenticate to GCS.

  - `project_id` The Google Cloud project ID of the project owning the above credentials and GCS bucket.

- `aws_s3:` The AWS section configures AWS S3 storage.

  - `region` The AWS region

  - `bucket` The AWS S3 bucket (will be created automatically)

  - `credentials_profile` If a profile other than default is chosen, use that one.

  - By default, the S3 blobstore will rely on environment variables, shared credentials, or IAM roles. See [AWS Go SDK docs](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials) for more information.

- `azure:` The Azure section configures Azure Storage.

  - `account_name` The name of the Azure storage account

  - `account_key` The key for the Azure storage account

  - `container_name` The name of the Azure storage container.

**Optional**

- `chunk_file_size_bytes:` How many bytes to buffer in memory before flushing a chunk of build protocol data to disk.

## Example sections

### Disk

```
storage:
  ttl_seconds: 86400  # One day in seconds.
  chunk_file_size_bytes: 3000000  # 3 MB
  disk:
    root_directory: /tmp/buildbuddy
```

### GCS

```
storage:
  ttl_seconds: 0  # No TTL.
  chunk_file_size_bytes: 3000000  # 3 MB
  gcs:
    bucket: "buildbuddy_blobs"
    project_id: "my-cool-project"
    credentials_file: "enterprise/config/my-cool-project-7a9d15f66e69.json"
```

### AWS S3

```
storage:
  aws_s3:
    # required
    region: "us-west-2"
    bucket: "buildbuddy-bucket"
    # optional
    credentials_profile: "other-profile"
```

### Minio

```
storage:
  aws_s3:
    static_credentials_id: "YOUR_MINIO_ACCESS_KEY"
    static_credentials_secret: "YOUR_MINIO_SECRET"
    endpoint: "http://localhost:9000"
    disable_ssl: true
    s3_force_path_style: true
    region: "us-east-1"
    bucket: "buildbuddy-storage-bucket"
```

### Azure

```
storage:
  azure:
    account_name: "mytestblobstore"
    account_key: "XXXxxxXXXxXXXXxxXXXXXxXXXXXxX"
    container_name: "my-container"
```
