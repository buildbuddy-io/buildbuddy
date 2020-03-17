# BuildBuddy Config File
Buildbuddy is configured using a yaml-formatted config file. On startup, BuildBuddy reads this file which is specified using the ```--config_file``` flag.

This single file is read only once, at startup.

You can edit this file with a text editor to configure BuildBuddy's behavior.

There are three types of config flags: **Required**, **Recommended**, and **Optional**.

* **Required** flags are required. BuildBuddy will not run without them.
* **Recommended** flags should be set -- if you unset them BuildBuddy will run but produce undefined output.
* **Optional** flags may be set to configure extra functionality. BuildBuddy will work fine without them.

# Configuration Options

## App

```app:``` **Recommended** The app section contains app-level options.

#### BuildBuddyURL

```build_buddy_url``` **Recommended** The BuildBuddyURL is a string that configures an external URL for where your BuildBuddy instance is hosted. This
is used when BuildBuddy generates links.

Example App Section:
```
app:
  build_buddy_url: "http://localhost:8080"
```

## BuildEventProxy
```build_event_proxy:``` **Optional** The BuildEventProxy section configures proxy behavior, allowing BuildBuddy to forward build events to other build-event-protocol compatible servers.

#### Hosts
```hosts``` **Optional** A list of host strings that BuildBudy should connect and forward events to.

Example BuildEventProxy section:
```
build_event_proxy:
  hosts:
    - "grpc://localhost:1985"
    - "grpc://events.buildbuddy.io:1985"
```

## Database
```database:``` **Required** The Database section configures the database that BuildBuddy stores all data in.

#### DataSource
```data_source``` **Required** This is a connection string used by the database driver to connect to the database.

Example Database section: (sqlite)
```
database:
  data_source: "sqlite3:///tmp/buildbuddy.db"
```

Example Database section: (mysql)
```
database:
  data_source: "mysql://buildbuddy:pAsSwOrD@tcp(12.34.56.78)/buildbuddy_db"
```

## Storage
```storage:``` **Required** The Storage section configures where and how BuildBuddy will store blob data.

#### Disk
```disk:``` **Optional** The Disk section configures disk-based blob storage.

##### RootDirectory
```root_directory``` **Optional** The root directory to store all blobs in, if using disk based storage. This directory must be writable and readable by the BuildBuddy process.

#### GCS
```gcs:``` **Optional** The GCS section configures Google Cloud Storage based blob storage.

##### Bucket
```bucket``` **Optional** The name of the GCS bucket to store files in. Will be created if it does not already exist.

##### CredentialsFile
```credentials_file``` **Optional** A path to a [JSON credentials file](https://cloud.google.com/docs/authentication/getting-started) that will be used to authenticate to GCS. 

##### ProjectID
```project_id``` **Optional** The Google Cloud project ID of the project owning the above credentials and GCS bucket.


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
```integrations:``` **Optional** A section configuring optional external services BuildBuddy can integrate with, like Slack.

#### Slack
```slack:``` **Optional** A section configuring Slack integration.

##### WebhookURL
```webhook_url``` **Optional** A webhook url to post build update messages to.

Example Integrations Section:
```
integrations:
  slack:
    webhook_url: "https://hooks.slack.com/services/AAAAAAAAA/BBBBBBBBB/1D36mNyB5nJFCBiFlIOUsKzkW"
```
