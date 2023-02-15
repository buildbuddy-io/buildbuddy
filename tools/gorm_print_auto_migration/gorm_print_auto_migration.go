package main

import (
	"bufio"
	"flag"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"gorm.io/gorm"

	bundle "github.com/buildbuddy-io/buildbuddy"
)

var (
	dataSource = flag.String("data_source", "sqlite3:///tmp/buildbuddy.db", "The SQL database to connect to, specified as a connection string.")
	outputPath = flag.String("output_path", "migration.txt", "The output file to write the gorm migration logs to.")
)

func main() {
	flag.Parse()

	bundleFS, err := bundle.Get()
	if err != nil {
		log.Fatalf("Error getting bundle FS: %s", err)
	}

	fileResolver := fileresolver.New(bundleFS, "")
	ds, driverName, err := db.OpenDB(fileResolver, *dataSource, &db.AdvancedConfig{})
	if err != nil {
		log.Fatalf("Error opening db: %s", err)
	}

	// Replace every gorm raw SQL command with a function that appends the SQL string to a slice
	sqlStrings := make([]string, 0)
	if err := ds.Callback().Raw().Replace("gorm:raw", func(db *gorm.DB) {
		sqlStrings = append(sqlStrings, db.Statement.SQL.String())
	}); err != nil {
		log.Fatalf("Error replacing gorm sql statements: %s", err)
	}

	if err := db.RunMigrations(driverName, ds); err != nil {
		log.Fatalf("Database auto-migration failed: %s", err)
	}

	file, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("Error creating file: %s", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, data := range sqlStrings {
		_, _ = w.WriteString(data + "\n")
	}
	err = w.Flush()
	if err != nil {
		log.Fatalf("Error flushing data to file: %s", err)
	}
}
