package database

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	// We support MySQL (preferred), Postgresql, and Sqlite3
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

var (
	autoMigrateDB = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
)

type Database struct {
	gormDB *gorm.DB
}

func NewDatabase(dialect string, args ...interface{}) (*Database, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
	gdb.LogMode(true)
	if *autoMigrateDB {
		gdb.AutoMigrate(tables.GetAllTables()...)
	}
	return &Database{
		gormDB: gdb,
	}, nil
}

func GetConfiguredDatabase(c *config.Configurator) (*Database, error) {
	datasource := c.GetDBDataSource()
	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		dialect, connString := parts[0], parts[1]
		return NewDatabase(dialect, connString)
	}
	return nil, fmt.Errorf("No database configured -- please specify at least one in the config")
}

// INVOCATIONS API

func (d *Database) InsertOrUpdateInvocation(ctx context.Context, ti *tables.Invocation) error {
	return d.gormDB.Transaction(func(tx *gorm.DB) error {
		var existing tables.Invocation
		if err := tx.Where("invocation_id = ?", ti.InvocationID).First(&existing).Error; err != nil {
			if gorm.IsRecordNotFoundError(err) {
				tx.Create(ti)
			}
		} else {
			tx.Model(&existing).Where("invocation_id = ?", ti.InvocationID).Updates(ti)
		}
		return nil
	})
}

func (d *Database) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	ti := &tables.Invocation{}
	err := d.gormDB.Transaction(func(tx *gorm.DB) error {
		existingRow := tx.Raw(`SELECT * FROM Invocations as i
                                       WHERE i.invocation_id = ?`, invocationID)
		if err := existingRow.Scan(ti).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *Database) LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error) {
	cutoffUsec := cutoffTime.UnixNano()
	rows, err := d.gormDB.Raw(`SELECT * FROM Invocations as i
                                   WHERE i.created_at_usec < ?
                                   LIMIT ?`, cutoffUsec, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	invocations := make([]*tables.Invocation, 0)
	var ti tables.Invocation
	for rows.Next() {
		if err := d.gormDB.ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		i := ti
		invocations = append(invocations, &i)
	}
	return invocations, nil
}

func (d *Database) DeleteInvocation(ctx context.Context, invocationID string) error {
	ti := &tables.Invocation{InvocationID: invocationID}
	return d.gormDB.Delete(ti).Error
}

// Not part of the Database interface because it's really just here to be used
// by SimpleSearcher.
func (d *Database) RawQueryInvocations(ctx context.Context, sql string, values ...interface{}) ([]*tables.Invocation, error) {
	rows, err := d.gormDB.Raw(sql, values...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invocations := make([]*tables.Invocation, 0)
	var ti tables.Invocation
	for rows.Next() {
		if err := d.gormDB.ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		i := ti
		invocations = append(invocations, &i)
	}
	return invocations, nil
}
