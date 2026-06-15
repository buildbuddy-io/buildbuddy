package clickhouse_schema_test

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/gormutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	gormclickhouse "gorm.io/driver/clickhouse"
)

func TestRunMigrations_DoesNotEmitNoopColumnChanges(t *testing.T) {
	db := openClickHouseDB(t)

	// Initial migrations create the schema from scratch.
	require.NoError(t, schema.RunMigrations(db))

	// A second migration should not produce any schema SQL. This catches GORM
	// nullable drift where ClickHouse non-null columns are repeatedly rewritten
	// to the same non-null type.
	sqlStrings := make([]string, 0)
	require.NoError(t, gormutil.RegisterLogSQLCallback(db, &sqlStrings))
	require.NoError(t, schema.RunMigrations(db))
	assert.Empty(t, sqlStrings)
}

func TestUsageViewMigration_CreateNoopAndReplace(t *testing.T) {
	db := openClickHouseDB(t)

	// Initial migrations should create both RawUsage and the Usage view.
	require.NoError(t, schema.RunMigrations(db))
	insertRawUsage(t, db, 5)
	assert.Equal(t, int64(5), queryUsageCount(t, db))

	// Running migrations again against the actual ClickHouse-stored view query
	// should be a no-op. This catches false drift caused by ClickHouse changing
	// the saved SELECT formatting in system.tables.as_select.
	initialModifiedAt := usageViewModifiedAt(t, db)
	waitForNextClickHouseMetadataTick()
	require.NoError(t, schema.RunMigrations(db))
	assert.Equal(t, initialModifiedAt, usageViewModifiedAt(t, db))

	// Simulate a manually drifted view definition. The next migration should
	// detect the different query and replace the view definition.
	require.NoError(t, db.Exec(`
		CREATE OR REPLACE VIEW "Usage" AS
		SELECT
			group_id,
			period_start,
			sku,
			labels,
			SUM(count) * 0 AS count
		FROM RawUsage FINAL
		GROUP BY
			group_id,
			period_start,
			sku,
			labels
	`).Error)
	assert.Equal(t, int64(0), queryUsageCount(t, db))

	driftedModifiedAt := usageViewModifiedAt(t, db)
	waitForNextClickHouseMetadataTick()
	require.NoError(t, schema.RunMigrations(db))
	assert.True(t, usageViewModifiedAt(t, db).After(driftedModifiedAt))
	assert.Equal(t, int64(5), queryUsageCount(t, db))
}

func openClickHouseDB(t *testing.T) *gorm.DB {
	dsn := testclickhouse.Start(t, true /*=reuseServer*/)
	options, err := clickhouse.ParseDSN(dsn)
	require.NoError(t, err)
	sqlDB := clickhouse.OpenDB(options)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})
	db, err := gorm.Open(schema.NewGORMDialector(gormclickhouse.Config{
		Conn:                         sqlDB,
		DontSupportEmptyDefaultValue: true,
	}))
	require.NoError(t, err)
	return db
}

func insertRawUsage(t *testing.T, db *gorm.DB, count int64) {
	err := db.Create(&schema.RawUsage{
		GroupID:     "GR1",
		SKU:         sku.BuildEventsBESCount,
		Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
		PeriodStart: time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC),
		BufferID:    "test:redis",
		Count:       count,
	}).Error
	require.NoError(t, err)
}

func queryUsageCount(t *testing.T, db *gorm.DB) int64 {
	row := struct {
		Count int64 `gorm:"column:count"`
	}{}
	err := db.Raw(`
		SELECT count
		FROM "Usage"
		WHERE group_id = ? AND sku = ?
	`, "GR1", sku.BuildEventsBESCount).Take(&row).Error
	require.NoError(t, err)
	return row.Count
}

func usageViewModifiedAt(t *testing.T, db *gorm.DB) time.Time {
	row := struct {
		MetadataModificationTime time.Time `gorm:"column:metadata_modification_time"`
	}{}
	err := db.Raw(`
		SELECT metadata_modification_time
		FROM system.tables
		WHERE database = currentDatabase()
			AND name = ?
			AND engine = 'View'
	`, (&schema.Usage{}).ViewName()).Take(&row).Error
	require.NoError(t, err)
	return row.MetadataModificationTime
}

func waitForNextClickHouseMetadataTick() {
	// system.tables.metadata_modification_time is a DateTime, so it only has
	// second precision. Wait just long enough for the next timestamp bucket.
	now := time.Now()
	time.Sleep(time.Second - time.Duration(now.Nanosecond()) + 10*time.Millisecond)
}
