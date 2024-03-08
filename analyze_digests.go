package main

import (
	"bufio"
	"log"
	"os"
	"slices"

	"gopkg.in/yaml.v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type Build struct {
	Events            []Event
	ActionDigests     map[string]bool
	TreeRootDigests   map[string]bool
	TreeCacheL0Misses int
}

type Event struct {
	UnixNanos       int64  `yaml:"t"`
	EventName       string `yaml:"event"`
	Status          string `yaml:"status"`
	ActionDigest    string `yaml:"digest"`
	RootDigest      string `yaml:"root_digest"`
	TreeCacheDigest string `yaml:"treecache_digest"`
	Level           int    `yaml:"level"`
}

func run() error {
	var records []Event
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		var record Event
		err := yaml.Unmarshal([]byte(line), &record)
		if err != nil {
			log.Fatalf("Error unmarshaling YAML: %s", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// Sort all records by timestamp
	slices.SortFunc(records, func(a, b Event) int {
		return int(a.UnixNanos - b.UnixNanos)
	})

	// Group records by build, based on START_BUILD markers
	var builds []*Build
	for _, r := range records {
		if r.EventName == "START_BUILD" {
			log.Printf("START_BUILD at %d", r.UnixNanos)
			b := &Build{
				ActionDigests:   map[string]bool{},
				TreeRootDigests: map[string]bool{},
			}
			builds = append(builds, b)
		}

		build := builds[len(builds)-1]
		build.Events = append(build.Events, r)
		if r.ActionDigest != "" {
			build.ActionDigests[r.ActionDigest] = true
		}
		if r.RootDigest != "" {
			build.TreeRootDigests[r.RootDigest] = true
		}
		if r.EventName == "MISS" && r.Level == 0 {
			build.TreeCacheL0Misses++
		}
	}

	// Compare action digests across the first and second builds
	newActionDigests := 0
	newRootDigests := 0
	for d := range builds[1].ActionDigests {
		if !builds[0].ActionDigests[d] {
			newActionDigests++
		}
	}
	for d := range builds[1].TreeRootDigests {
		if !builds[0].TreeRootDigests[d] {
			newRootDigests++
		}
	}
	log.Printf("New action digests in second build: %d", newActionDigests)
	log.Printf("New GetTree root digests in second build: %d", newRootDigests)
	log.Printf("TreeCache L0 misses in second build: %d", builds[1].TreeCacheL0Misses)
	return nil
}
