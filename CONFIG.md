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

```app:``` The app section contains app-level options.

### BuildBuddyURL

```build_buddy_url``` **Recommended** The BuildBuddyURL is a string that configures an external URL for where your BuildBuddy instance is hosted. This
is used when BuildBuddy generates links.

Example App Section:
```
app:
  build_buddy_url: "http://localhost:8080"
```

## BuildEventProxy
```build_event_proxy:``` The BuildEventProxy section configures proxy behavior, allowing BuildBuddy to forward build events to other build-event-protocol compatible servers.

### Hosts
```hosts``` **Optional** A list of host strings that BuildBudy should connect and forward events to.

## Database
```database:``` The Database section configures the database that BuildBuddy stores all data in.

### DataSource
```data_source```
type generalConfig struct {
        Database        databaseConfig     `yaml:"database"`
        Storage         storageConfig      `yaml:"storage"`
        Integrations    integrationsConfig `yaml:"integrations"`
}

type appConfig struct {
        BuildBuddyURL string `yaml:"build_buddy_url"`
}

type buildEventProxy struct {
        Hosts []string `yaml:"hosts"`
}

type databaseConfig struct {
        DataSource string `yaml:"data_source"`
}

type storageConfig struct {
        Disk               DiskConfig `yaml:"disk"`
        GCS                GCSConfig  `yaml:"gcs"`
        TTLSeconds         int        `yaml:"ttl_seconds"`
        ChunkFileSizeBytes int        `yaml:"chunk_file_size_bytes"`
}

type DiskConfig struct {
        RootDirectory string `yaml:"root_directory"`
}

type GCSConfig struct {
        Bucket          string `yaml:"bucket"`
        CredentialsFile string `yaml:"credentials_file"`
        ProjectID       string `yaml:"project_id"`
}

type integrationsConfig struct {
        Slack SlackConfig `yaml:"slack"`
}

type SlackConfig struct {
        WebhookURL string `yaml:"webhook_url"`
}
