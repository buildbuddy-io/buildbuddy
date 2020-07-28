<!--
{
  "name": "Enterprise Setup",
  "category": "5eed3e5fa3f1277a6b94b83a",
  "priority": 900
}
-->
# Getting Started with BuildBuddy Enterprise

We’re here to help you get started -- if you have questions please don’t hesitate to email us at [setup@buildbuddy.io](setup@buildbuddy.io) or ping us on our [Slack channel](https://join.slack.com/t/buildbuddy/shared_invite/zt-e0cugoo1-GiHaFuzzOYBPQzl9rkUR_g).


## Installation

The BuildBuddy Enterprise app container is published to gcr.io. Installing on your Kubernetes cluster is easy -- just authenticate to your cluster (1gcloud container clusters get-credentials` or similar) and run the following commands to install BuildBuddy Enterprise:

1. `git clone https://github.com/buildbuddy-io/buildbuddy.git`
1. `cd buildbuddy`
1. `./k8s_on_prem.sh -enterprise`

Many of the configurations below require using a custom BuildBuddy configuration file. To make this easier, the `k8s_on_prem.sh` script can optionally push a config file to your cluster in a Kubernetes ConfigMap that contains the contents of a custom config file. To do this, just specify the -config flag with an argument that is the path to your custom configuration file. For example:

1. `./k8s_on_prem.sh -enterprise -config foo/bar/buildbuddy.custom.yaml`

## Configuration
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

### GCS Based Cache / Object Storage

By default, BuildBuddy will cache objects and store uploaded build events on the local disk. If you want to store them in a shared durable location, like a Google Cloud Storage bucket, you can do that by configuring a GCS cache or storage backend.

If your BuildBuddy instance is running on a machine with Google Default Credentials, no credentials file will be necessary. If not, you should [create a service account](https://cloud.google.com/docs/authentication/getting-started) with permissions to write to cloud storage, and download the credentials .json file. The configuration below configures a cloud storage bucket to act as a storage backend and cache:

```
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
```

## Learn more
For more information on configuring BuildBuddy, see our [Configuration docs](config.m). If you have questions please don’t hesitate to email us at [setup@buildbuddy.io](setup@buildbuddy.io) or ping us on our [Slack channel](https://join.slack.com/t/buildbuddy/shared_invite/zt-e0cugoo1-GiHaFuzzOYBPQzl9rkUR_g).
