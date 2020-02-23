package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// When adding new storage fields, always be explicit about their yaml field
// name.
type generalConfig struct {
	Database databaseConfig `yaml:"database"`
	Storage  storageConfig  `yaml:"storage"`
}

type databaseConfig struct {
	DataSource string `yaml:"data_source"`
}

type storageConfig struct {
	Disk DiskConfig `yaml:"disk"`
	GCS  GCSConfig  `yaml:"gcs"`
}

type DiskConfig struct {
	RootDirectory string `yaml:"root_directory"`
	TtlSeconds    int64  `yaml:"ttl_seconds"`
}

type GCSConfig struct {
	Bucket          string `yaml:"bucket"`
	CredentialsFile string `yaml:"credentials_file"`
	ProjectID       string `yaml:"project_id"`
}

func ensureDirectoryExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Printf("Directory '%s' did not exist; creating it.", dir)
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func readConfig(fullConfigPath string) (*generalConfig, error) {
	_, err := os.Stat(fullConfigPath)

	// If the file does not exist then we are SOL.
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Config file %s not found", fullConfigPath)
	}

	fileBytes, err := ioutil.ReadFile(fullConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error reading config file: %s", err)
	}

	var gc generalConfig
	if err := yaml.Unmarshal([]byte(fileBytes), &gc); err != nil {
		return nil, fmt.Errorf("Error parsing config file: %s", err)
	}
	return &gc, nil
}

func validateConfig(c *generalConfig) error {
	if c.Storage.Disk.RootDirectory != "" {
		if err := ensureDirectoryExists(c.Storage.Disk.RootDirectory); err != nil {
			return err
		}
	}
	return nil
}

type Configurator struct {
	fullConfigPath string
	lastReadTime   time.Time
	gc             *generalConfig
}

func NewConfigurator(configFilePath string) (*Configurator, error) {
	log.Printf("Reading buildbuddy config from '%s'", configFilePath)
	conf, err := readConfig(configFilePath)
	if err != nil {
		return nil, err
	}
	if err := validateConfig(conf); err != nil {
		return nil, err
	}
	return &Configurator{
		fullConfigPath: configFilePath,
		lastReadTime:   time.Now(),
		gc:             conf,
	}, nil
}

func (c *Configurator) rereadIfStale() {
	stat, err := os.Stat(c.fullConfigPath)
	if err != nil {
		log.Printf("Error STATing config file: %s", err)
		return
	}
	// We already read this thing.
	if c.lastReadTime.After(stat.ModTime()) {
		return
	}
	conf, err := readConfig(c.fullConfigPath)
	if err != nil {
		log.Printf("Error rereading config file: %s", err)
		return
	}
	c.gc = conf
}

func (c *Configurator) GetStorageDiskRootDir() string {
	c.rereadIfStale()
	return c.gc.Storage.Disk.RootDirectory
}

func (c *Configurator) GetStorageGCSConfig() *GCSConfig {
	c.rereadIfStale()
	return &c.gc.Storage.GCS
}

func (c *Configurator) GetDBDataSource() string {
	c.rereadIfStale()
	return c.gc.Database.DataSource
}
