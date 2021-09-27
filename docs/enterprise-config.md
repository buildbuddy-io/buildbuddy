---
id: enterprise-config
title: Configuring BuildBuddy Enterprise
sidebar_label: Enterprise Configuration
---

BuildBuddy Enterprise allows configuration of many features that are not available in the open-core version. Below you’ll find examples for configuring some of these features. If you don’t see what you’re looking for below, please don’t hesitate to ask us! For a full overview of what can be configured, see our [Configuration docs](config.md).

### MySQL Data Storage

BuildBuddy uses a SQL connection string to specify the database it will connect to. An example string is:

```
"mysql://user:pass@tcp(12.34.56.78)/database_name"
```

To connect BuildBuddy to your own MySQL server:

1. Create a new database on your MySQL server
1. Create a new user with full access to that database
1. Put the username, password, IP address of your MySQL server, and database name into the BuildBuddy data_source connection string:

```
app:
  build_buddy_url: "https://app.buildbuddy.mydomain.com"
  events_api_url: "grpcs://events.buildbuddy.mydomain.com:1986"
  cache_api_url: "grpcs://cache.buildbuddy.mydomain.com:1986"
database:
  data_source: "mysql://user:pass@tcp(12.34.56.78)/database_name"
```

If using the [BuildBuddy Enterprise Helm charts](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise), MySQL can be configured for you using the `mysql.enabled`, `mysql.username`, and `mysql.password` values.

### Default Redis Target

For a BuildBuddy deployment running multiple apps, it is necessary to provide a default redis target for some features to work correctly. Metrics collection, usage tracking, and responsive build logs all depend on this.

If no default redis target is configured, we will fall back to using the cache redis target, if available, and then the remote execution target, if available. The default redis target also acts as the primary fallback if the remote execution redis target is left unspecified. The default redis target does NOT act as a fallback for the cache redis target.

The configuration below demostrates a default redis target:

```
app:
  default_redis_target: "my-redis.local:6379"
```

### GCS Based Cache / Object Storage / Redis

By default, BuildBuddy will cache objects and store uploaded build events on the local disk. If you want to store them in a shared durable location, like a Google Cloud Storage bucket, you can do that by configuring a GCS cache or storage backend.

If your BuildBuddy instance is running on a machine with Google Default Credentials, no credentials file will be necessary. If not, you should [create a service account](https://cloud.google.com/docs/authentication/getting-started) with permissions to write to cloud storage, and download the credentials .json file.

We also recommend providing a Redis instance for improved remote build execution & small file performance. This can be configured automatically using the [BuildBuddy Enterprise Helm charts](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise) with the `redis.enabled` value.

The configuration below configures Redis & GCS storage bucket to act as a storage backend and cache:

```
storage:
  ttl_seconds: 2592000  # 30 days
  chunk_file_size_bytes: 3000000  # 3 MB
  gcs:
    bucket: "buildbuddy_prod_blobs"
    project_id: "flame-build"
    credentials_file: "your_service-acct.json"
cache:
  redis_target: "my-redis.local:6379"
  gcs:
    bucket: "buildbuddy_cache"
    project_id: "your_gcs_project_id"
    credentials_file: "/path/to/your/credential/file.json"
    ttl_days: 30
```

If using Amazon S3, you can configure your storage and cache similarly:

```
storage:
  ttl_seconds: 2592000  # 30 days
  chunk_file_size_bytes: 3000000  # 3 MB
  aws_s3:
    region: "us-west-2"
    bucket: "buildbuddy-bucket"
    credentials_profile: "other-profile"
cache:
  redis_target: "my-redis.local:6379"
  s3:
    region: "us-west-2"
    bucket: "buildbuddy-bucket"
    credentials_profile: "other-profile"
    ttl_days: 30
```

### Authentication Provider Integration

BuildBuddy supports OpenID Connect (OIDC) as a way of interacting with an Auth Provider like Google, Okta, or similar to authenticate your users when they log in. Configuring this is easy, below is an example of using BuildBuddy with Okta. Configuring your Auth Provider to support OIDC is outside the scope of this doc, but we’ve done it for Google, Okta, and others, and are happy to lend a helping hand if you’re stuck.

```
auth:
  oauth_providers:
    - issuer_url: "https://your-custom-domain.okta.com"
      client_id: "0aaa5twc0asdkUW123x6"
      client_secret: "P8fRAYxWMmG9asd040GV2_q9MZ6esTJif1n4BubxU"
```

Here’s another example of Google login using credentials obtained from: https://console.developers.google.com/apis/credentials

```
auth:
  oauth_providers:
    - issuer_url: "https://accounts.google.com"
      client_id: "YOUR_CLIENT_ID.apps.googleusercontent.com"
      Client_secret: "YOUR_CLIENT_SECRET"
```

### Certificate Based Authentication

Your users can authenticate to BuildBuddy using an API key or they can use Certificate based authentication over mTLS. To configure mTLS, you must generate a new server certificate authority and key. You can do this using the `openssl` command, for example:

```
# Change these CN's to match your BuildBuddy host name
SERVER_SUBJECT=buildbuddy.io
PASS=$(openssl rand -base64 32) # <- Save this :)

# Generates ca.key
openssl genrsa -passout pass:${PASS} -des3 -out ca.key 4096

# Generates ca.crt
openssl req -passin pass:${PASS} -new -x509 -days 365000 -key ca.key -out ca.crt -subj "/CN=${SERVER_SUBJECT}"

# Generates ca.pem
openssl pkcs8 -passin pass:${PASS} -topk8 -nocrypt -in ca.key -out ca.pem
```

Then, you can use the generated ca.csr and ca.pem files in your BuildBuddy configuration like this:

```
ssl:
  enable_ssl: true
  client_ca_cert_file: your_ca.crt
  client_ca_key_file: your_ca.pem
```

### Remote Build Execution

To enable Remote Build Execution, you'll need to add the following to your config.yaml:

```
remote_execution:
  enable_remote_exec: true
```

You'll also need to deploy executors to handle remote builds. The recommended way of deploying these is using our [Enterprise Helm Chart](enterprise-helm.md).

For more information on configuring on-prem RBE, see our [enterprise on-prem RBE setup docs](enterprise-rbe.md).

### Putting It All Together

Here’s what a fully-featured config.yaml looks like which includes all of the features listed above.

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
    gcs:
      bucket: "buildbuddy_cache"
      project_id: "your_gcs_project_id"
      credentials_file: "/path/to/your/credential/file.json"
      ttl_days: 30
auth:
  oauth_providers:
    - issuer_url: "https://your-custom-domain.okta.com"
      client_id: "0aaa5twc0asdkUW123x6"
      client_secret: "P8fRAYxWMmG9asd040GV2_q9MZ6esTJif1n4BubxU"
ssl:
  enable_ssl: true
  client_ca_cert_file: your_ca.crt
  client_ca_key_file: your_ca.pem
remote_execution:
  enable_remote_exec: true
```

## Learn more

For more information on configuring BuildBuddy, see our [Configuration docs](config.md). If you have questions please don’t hesitate to email us at [setup@buildbuddy.io](mailto:setup@buildbuddy.io) or ping us on our [Slack channel](https://slack.buildbuddy.io).
