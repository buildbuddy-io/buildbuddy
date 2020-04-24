# BuildBuddy Config File
Buildbuddy is configured using a yaml-formatted configuration file. On startup, BuildBuddy reads this config file which is specified using the ```--config_file``` flag. The config file is periodically re-read, although some options like enabling or disabling a cache require a restart to take effect.

If you're running BuildBuddy in a Docker image - you can use Docker's [-v flag](https://docs.docker.com/storage/volumes/) to map a custom local config file to ```/config.yaml``` in the Docker image:
```
docker pull gcr.io/flame-public/buildbuddy-app-onprem:latest && docker run -p 1985:1985 -p 8080:8080 -v /PATH_TO_YOUR_LOCAL_CONFIG/config.yaml:/config.yaml gcr.io/flame-public/buildbuddy-app-onprem:latest
```

You can edit this file with a text editor to configure BuildBuddy's behavior. Here are some sample configuration files that we've used in the past, and below that a detailed list of the config options and what they do.

# Sample Configuration Files

## Running locally (disk only)
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

## Running with MySQL and in-memory cache
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

## Running with your own Auth provider (OIDC) [ENTERPRISE VERSION]
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

# Configuration File Options

There are three types of config flags: *Required*, *Recommended*, and *Optional*.

* *Required* - BuildBuddy will not run without these.
* *Recommended* - BuildBuddy will run without them but may produce undefined output.
* *Optional* - They configure optional functionality. BuildBuddy will happily run without them.


## App

```app:``` *Recommended* The app section contains app-level options.

#### BuildBuddyURL

```build_buddy_url``` *Recommended* Configures the external URL where your BuildBuddy instance can be found. This is used by BuildBuddy to generate links.

```events_api_url``` *Optional* Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

```cache_api_url``` *Optional* Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

Example App Section:
```
app:
  build_buddy_url: "http://acme.corp"
```

## BuildEventProxy
```build_event_proxy:``` *Optional* The BuildEventProxy section configures proxy behavior, allowing BuildBuddy to forward build events to other build-event-protocol compatible servers.

#### Hosts
```hosts``` *Optional* A list of host strings that BuildBudy should connect and forward events to.

Example BuildEventProxy section:
```
build_event_proxy:
  hosts:
    - "grpc://localhost:1985"
    - "grpc://events.buildbuddy.io:1985"
```

## Database
```database:``` *Required* The Database section configures the database that BuildBuddy stores all data in.

#### DataSource
```data_source``` *Required* This is a connection string used by the database driver to connect to the database.

Example Database section: (sqlite)
```
database:
  data_source: "sqlite3:///tmp/buildbuddy.db"
```

Example Database section: (mysql)
```
database:
  data_source: "mysql://buildbuddy_user:pAsSwOrD@tcp(12.34.56.78)/buildbuddy_db"
```

## Storage
```storage:``` *Required* The Storage section configures where and how BuildBuddy will store blob data.

#### ChunkFileSizeBytes
```chunk_file_size_bytes:``` *Optional* How many bytes to buffer in memory before flushing a chunk of build protocol data to disk.

#### Disk
```disk:``` *Optional* The Disk section configures disk-based blob storage.

##### RootDirectory
```root_directory``` *Optional* The root directory to store all blobs in, if using disk based storage. This directory must be readable and writable by the BuildBuddy process. THe directory will be created if it does not exist.

#### GCS
```gcs:``` *Optional* The GCS section configures Google Cloud Storage based blob storage.

##### Bucket
```bucket``` *Optional* The name of the GCS bucket to store files in. Will be created if it does not already exist.

##### CredentialsFile
```credentials_file``` *Optional* A path to a [JSON credentials file](https://cloud.google.com/docs/authentication/getting-started) that will be used to authenticate to GCS. 

##### ProjectID
```project_id``` *Optional* The Google Cloud project ID of the project owning the above credentials and GCS bucket.


Example Storage section: (disk)
```
storage:
  ttl_seconds: 86400  # One day in seconds.
  chunk_file_size_bytes: 3000000  # 3 MB
  disk:
    root_directory: /tmp/buildbuddy
```


Example Storage section: (gcs)
```
storage:
  ttl_seconds: 0  # No TTL.
  chunk_file_size_bytes: 3000000  # 3 MB
  gcs:
    bucket: "buildbuddy_blobs"
    project_id: "my-cool-project"
    credentials_file: "enterprise/config/my-cool-project-7a9d15f66e69.json"
```

## Integrations
```integrations:``` *Optional* A section configuring optional external services BuildBuddy can integrate with, like Slack.

#### Slack
```slack:``` *Optional* A section configuring Slack integration.

##### WebhookURL
```webhook_url``` *Optional* A webhook url to post build update messages to.

Example Integrations Section:
```
integrations:
  slack:
    webhook_url: "https://hooks.slack.com/services/AAAAAAAAA/BBBBBBBBB/1D36mNyB5nJFCBiFlIOUsKzkW"
```

## Cache
```cache:``` *Optional* The Cache section enables the BuildBuddy cache and configures how and where it will store data.

#### MaxSizeBytes
```max_size_bytes:``` *Optional* How big to allow the cache to be.

#### InMemory
```in_memory:``` *Optional* Whether or not to use the in_memory cache.

#### Disk
```disk:``` *Optional* The Disk section configures disk-based cache.

##### RootDirectory
```root_directory``` *Optional* The root directory to store cache data in, if using the disk cache. This directory must be readable and writable by the BuildBuddy process. THe directory will be created if it does not exist.


## Auth [ENTERPRISE]
```auth:``` *Optional* The Auth section enables BuildBuddy authentication using an OpenID Connect provider that you specify.

#### OauthProviders
```oauth_providers:``` A section for the configured OAuth Providers.

##### OauthProvider 
```-issuer_url: ``` The issuer URL of this OIDC Provider.
``` client_id: ``` The oauth client ID.
``` client_secret: ``` The oauth client secret.


# BuildBuddy Flags

There are several configuration options that are not yet specified in the BuildBuddy configuration file. These are:

* ```--listen``` The interface that BuildBuddy will listen on. Defaults to 0.0.0.0 (all interfaces)
* ```--port``` The port to listen for HTTP traffic on. Defaults to 8080.
* ```--grpc_port``` The port to listen for gRPC traffic on. Defaults to 1985.

