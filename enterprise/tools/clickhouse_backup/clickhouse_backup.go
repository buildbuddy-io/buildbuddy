package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// Make sure to also configure:
// - olap_database.* flags
// - storage.* flags (bucket and path_prefix must match disk definition in clickhouse config)
var (
	create               = flag.NewFlagSet("create", flag.ExitOnError)
	createDatabase       = create.String("database", "", "Name of the database to backup.")
	createBackupDiskName = create.String("backup_disk_name", "", "Name of the disk to backup to. Must be configured in ClickHouse server config.")
	createFullBackupDay  = create.Int("full_backup_day_of_month", 1, "Day of the month to take a full backup. Avoids long chains of incremental backups.")

	// TODO: manually optimize and freeze old partitions before taking each
	// backup. For now, we run the backup script a few hours after the end of
	// the previous month (UTC) to increase the chance that no background merges
	// will modify the previous month's partitions (and invalidate large
	// portions of our full backup that we're taking on the first of the month).

	restore                    = flag.NewFlagSet("restore", flag.ExitOnError)
	restoreBackupDiskName      = restore.String("backup_disk_name", "", "Name of the disk to restore from. Must be configured in ClickHouse server config.")
	restoreBackupName          = restore.String("backup_name", "", "Name of the backup to restore from. If unset, the latest backup will be used.")
	restoreBackupDatabase      = restore.String("backup_database", "", "Name of the database to restore from. This should match the name passed to --database when creating the backup.")
	restoreDestinationDatabase = restore.String("destination_database", "", "Name of the database to restore to.")
	// WARNING: this flag should be used with caution - read the entire help
	// text before using it.
	restoreAllowNonEmptyTables = restore.Bool("allow_non_empty_tables", false, "Allow restoring to a non-empty table. This may be useful in cases where data was completely deleted, and we want to keep any rows that were created after the accidental deletion. This should NOT be used in cases of partial data loss, since it will result in duplicate rows in the restored table.")
	// Must set exactly one of these:
	restoreAllTables = restore.Bool("all_tables", false, "Restore all tables from the backup database. If false, only the tables specified via the 'tables' flag will be restored.")
	restoreTables    = flag.New(restore, "table", []string{}, "List of tables to restore from the backup database.")

	// TODO: list command (list backups)
	// TODO: clean command (remove old backups)
)

func main() {
	// Parse global flags and set up common dependencies
	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	flag.Parse()
	if err := config.Load(); err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}
	env := real_environment.NewBatchEnv()
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		log.Fatalf("Failed to disable auto-migration: %s", err)
	}
	if err := clickhouse.Register(env); err != nil {
		log.Fatal(err.Error())
	}
	if env.GetOLAPDBHandle() == nil {
		log.Fatal("OLAP database not configured")
	}
	if err := blobstore.Register(env); err != nil {
		log.Fatal(err.Error())
	}
	if env.GetBlobstore() == nil {
		log.Fatal("Blobstore not configured")
	}

	// Parse and run subcommand
	subcommand := flag.Args()
	if len(subcommand) == 0 {
		usage()
	}
	for _, c := range []struct {
		flags *flag.FlagSet
		run   func(ctx context.Context, env environment.Env) error
	}{
		{create, runSave},
		{restore, runRestore},
	} {
		if subcommand[0] == c.flags.Name() {
			if err := c.flags.Parse(subcommand[1:]); err != nil {
				log.Fatal("Failed to parse flags: " + err.Error())
			}
			if err := c.run(ctx, env); err != nil {
				log.Fatalf("Error: %s: %s", c.flags.Name(), err.Error())
			}
			return
		}
	}
	usage()
}

func usage() {
	log.Fatal("Usage: clickhouse_backup <create|restore>")
}

func runSave(ctx context.Context, env environment.Env) error {
	if *createDatabase == "" {
		return fmt.Errorf("missing database flag")
	}
	if *createBackupDiskName == "" {
		return fmt.Errorf("missing backup_disk_name flag")
	}

	nowUTC := env.GetClock().Now().UTC()
	// For now, associate the backup with 00:00:00 of the day the backup is
	// taken. Blobstore doesn't have the ability to list blobs (yet) so we check
	// for previous backups by looking for backups taken at 00:00:00 for the
	// past few days. In the future, if we want to take multiple backups per
	// day, and if we add ListBlobs support, then we can start recording the
	// exact timestamp, and these new backups will be backwards-compatible with
	// existing backups.
	backupName := backupNameForTimestamp(toStartOfDay(nowUTC))

	qStr := `BACKUP DATABASE ` + *createDatabase + ` TO Disk(?, ?)`
	qArgs := []any{*createBackupDiskName, backupName}
	incremental := false
	// If it's not the scheduled full-backup day, use the previous day's backup
	// as the base for the incremental backup, if it exists.
	if nowUTC.Day() != *createFullBackupDay {
		yesterdayBackupName := backupNameForTimestamp(toStartOfDay(nowUTC.AddDate(0, 0, -1)))
		// Check if yesterday's backup exists.
		if exists, err := backupExists(ctx, env, yesterdayBackupName, *createDatabase); err != nil {
			return fmt.Errorf("check blob exists: %w", err)
		} else if exists {
			log.Infof("Using previous backup %q as the base for the incremental backup", yesterdayBackupName)
			incremental = true
			qStr += ` SETTINGS base_backup = Disk(?, ?)`
			qArgs = append(qArgs, *createBackupDiskName, yesterdayBackupName)
		}
	}
	label := "create_backup"
	if incremental {
		label = "create_incremental_backup"
	}
	log.Infof("Backing up database %q to Disk(%q, %q) (incremental=%t)", *createDatabase, *createBackupDiskName, backupName, incremental)
	if err := env.GetOLAPDBHandle().NewQuery(ctx, label).Raw(qStr, qArgs...).Exec().Error; err != nil {
		return fmt.Errorf("run backup query: %w", err)
	}

	log.Infof("Backup completed in %s", env.GetClock().Since(nowUTC))

	return nil
}

func toStartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func backupNameForTimestamp(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z")
}

func runRestore(ctx context.Context, env environment.Env) error {
	if *restoreBackupDiskName == "" {
		return fmt.Errorf(`missing "disk_name" option`)
	}
	if *restoreBackupDatabase == "" {
		return fmt.Errorf(`missing "backup_database" option`)
	}
	if *restoreDestinationDatabase == "" {
		return fmt.Errorf(`missing "destination_database" option`)
	}
	if *restoreAllTables && len(*restoreTables) > 0 {
		return fmt.Errorf(`cannot set both "all_tables" and "tables" options`)
	}
	if !*restoreAllTables && len(*restoreTables) == 0 {
		return fmt.Errorf(`missing either "all_tables" or "tables" options`)
	}
	if *restoreBackupName == "" {
		log.Infof("No backup name provided, looking for recent backup")
		nowUTC := env.GetClock().Now().UTC()
		todayBackupName := backupNameForTimestamp(toStartOfDay(nowUTC))
		yesterdayBackupName := backupNameForTimestamp(toStartOfDay(nowUTC.AddDate(0, 0, -1)))
		// Just check today's or yesterday's for now.
		// TODO: support more fine-grained backups (e.g. list all backups and
		// pick the most recent one)
		backupsToCheck := []string{todayBackupName, yesterdayBackupName}
		for _, name := range backupsToCheck {
			if exists, err := backupExists(ctx, env, name, *restoreBackupDatabase); err != nil {
				return fmt.Errorf("check blob exists: %w", err)
			} else if exists {
				*restoreBackupName = name
				break
			}
		}
		if *restoreBackupName == "" {
			return fmt.Errorf(`no recent backups found and "backup_name" option is not set`)
		}
	}

	// NOTE: we don't set the allow_non_empty_tables setting here because we
	// don't always filter out duplicate rows correctly yet (e.g. using FINAL).
	if *restoreAllTables {
		src := backtickQuote(*restoreBackupDatabase)
		dst := backtickQuote(*restoreDestinationDatabase)
		qStr := `RESTORE DATABASE ` + src + ` AS ` + dst + ` FROM Disk(?, ?) SETTINGS allow_non_empty_tables = ?`
		qArgs := []any{*restoreBackupDiskName, *restoreBackupName, *restoreAllowNonEmptyTables}

		log.Infof("Restoring database %s from Disk(%q, %q), backup database %s", dst, *restoreBackupDiskName, *restoreBackupName, src)
		start := env.GetClock().Now()
		if err := env.GetOLAPDBHandle().NewQuery(ctx, "restore_backup").Raw(qStr, qArgs...).Exec().Error; err != nil {
			return fmt.Errorf("restore database: %w", err)
		}
		log.Infof("Restored database %s in %s", dst, env.GetClock().Since(start))
	} else {
		for _, tableName := range *restoreTables {
			src := backtickQuote(*restoreBackupDatabase) + "." + backtickQuote(tableName)
			dst := backtickQuote(*restoreDestinationDatabase) + "." + backtickQuote(tableName)
			qStr := `RESTORE TABLE ` + src + ` AS ` + dst + ` FROM Disk(?, ?) SETTINGS allow_non_empty_tables = ?`
			qArgs := []any{*restoreBackupDiskName, *restoreBackupName, *restoreAllowNonEmptyTables}

			log.Infof("Restoring table %s from backup Disk(%q, %q), backup table %s", dst, *restoreBackupDiskName, *restoreBackupName, src)
			start := env.GetClock().Now()
			if err := env.GetOLAPDBHandle().NewQuery(ctx, "restore_table").Raw(qStr, qArgs...).Exec().Error; err != nil {
				return fmt.Errorf("restore table: %w", err)
			}
			log.Infof("Restored table %s in %s", dst, env.GetClock().Since(start))
		}
	}
	return nil
}

func backupExists(ctx context.Context, env environment.Env, backupName, databaseName string) (bool, error) {
	blobName := backupName + "/metadata/" + databaseName + ".sql"
	log.Infof("Checking if backup %q exists", blobName)
	return env.GetBlobstore().BlobExists(ctx, blobName)
}

func backtickQuote(s string) string {
	return "`" + s + "`"
}
