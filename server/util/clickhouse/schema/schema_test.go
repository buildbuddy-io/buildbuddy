package schema

import (
	"encoding/hex"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaInSync(t *testing.T) {
	tests := []struct {
		clickhouseTable Table
		primaryDBTable  interface{}
	}{
		{
			clickhouseTable: &Invocation{},
			primaryDBTable:  tables.Invocation{},
		},
		{
			clickhouseTable: &AllInvocation{},
			primaryDBTable:  tables.Invocation{},
		},
		{
			clickhouseTable: &Execution{},
			primaryDBTable:  tables.Execution{},
		},
		{
			clickhouseTable: &TestTargetStatus{},
			// don't testing schema in sync
			primaryDBTable: nil,
		},
		{
			clickhouseTable: &AuditLog{},
			// Not in primary DB.
			primaryDBTable: nil,
		},
		{
			clickhouseTable: &RawUsage{},
			// Not in primary DB. There is a Usage table but it has an
			// incompatible schema.
			primaryDBTable: nil,
		},
	}

	assert.Equal(t, len(getAllTables()), len(tests), "All clickhouse tables should be present in the tests")

	for _, tc := range tests {
		if tc.primaryDBTable == nil {
			continue
		}

		chType := reflect.Indirect(reflect.ValueOf(tc.clickhouseTable)).Type()

		t.Run(chType.Name(), func(t *testing.T) {
			primaryDBType := reflect.TypeOf(tc.primaryDBTable)

			chFields := reflect.VisibleFields(chType)
			primaryFields := reflect.VisibleFields(primaryDBType)

			// Check AdditionalFields().
			additionalFields := tc.clickhouseTable.AdditionalFields()
			for _, f := range additionalFields {
				_, found := chType.FieldByName(f)
				assert.True(t, found, "Field %q is not found in %s, but it's in AdditionalFields()", f, chType)
			}

			// Check ExcludedFields().
			excludedFields := tc.clickhouseTable.ExcludedFields()
			for _, f := range excludedFields {
				_, found := primaryDBType.FieldByName(f)
				assert.True(t, found, "Field %q is not found in %s, but it's in ExcludedFields()", f, primaryDBType)
			}
			for _, chField := range chFields {
				_, found := primaryDBType.FieldByName(chField.Name)
				isIncluded := slices.Contains(additionalFields, chField.Name)
				if isIncluded {
					assert.False(t, found, "Field %q is found in %s, but it's marked as additional", chField.Name, primaryDBType)
				} else {
					assert.True(t, found, "Field %q is not found in %s", chField.Name, primaryDBType)
				}
			}

			for _, primaryField := range primaryFields {
				if primaryField.Anonymous {
					continue
				}

				_, found := chType.FieldByName(primaryField.Name)
				isExcluded := slices.Contains(excludedFields, primaryField.Name)
				if isExcluded {
					assert.False(t, found, "Field %q is found in %s, but it's marked as excluded", primaryField.Name, chType)
				} else {
					assert.True(t, found, "Field %q is not found in %s", primaryField.Name, chType)
				}
				assert.NotEqual(t, found, isExcluded, "Field %q is not found in %s", primaryField.Name, chType)
			}
		})
	}
}

var NonStandardInvocationCopyFields = []string{
	"InvocationUUID",
	"Tags",
}

func TestToInvocationFromPrimaryDB(t *testing.T) {
	src := &tables.Invocation{}
	err := faker.FakeData(src)
	require.NoError(t, err)
	dest := ToInvocationFromPrimaryDB(src)

	primaryInvType := reflect.TypeOf(*src)
	primaryInvFields := reflect.VisibleFields(primaryInvType)
	srcValue := reflect.ValueOf(*src)
	destValue := reflect.ValueOf(*dest)
	excludedFields := (&Invocation{}).ExcludedFields()

	expectedUUID := hex.EncodeToString(src.InvocationUUID)
	assert.Equal(t, dest.InvocationUUID, expectedUUID, "src and dest have different values for field 'InvocationUUID'. src = %v, dest = %v", src.InvocationUUID, expectedUUID)
	// faker might pick a string with commas in it, so we unfortunately have to split here, too.
	assert.Equal(t, dest.Tags, strings.Split(src.Tags, ","))

	for _, primaryField := range primaryInvFields {
		if slices.Contains(excludedFields, primaryField.Name) || primaryField.Anonymous || slices.Contains(NonStandardInvocationCopyFields, primaryField.Name) {
			// already checked fields that don't do direct copies seperately above.
			continue
		}
		srcFieldValue := srcValue.FieldByName(primaryField.Name)
		destFieldValue := destValue.FieldByName(primaryField.Name)
		require.True(t, srcFieldValue.IsValid())
		if destFieldValue.IsValid() {
			assert.Equal(t, srcFieldValue.Interface(), destFieldValue.Interface(), "src and dest have different values for field %q. src = %v, dest = %v", primaryField.Name, srcFieldValue, destFieldValue)
		} else {
			assert.Failf(t, "dest has invalid field %q", primaryField.Name)
		}
	}
}

func TestToAllInvocationFromPrimaryDB(t *testing.T) {
	src := &tables.Invocation{}
	err := faker.FakeData(src)
	require.NoError(t, err)
	// Use a realistic group ID and UUID so we can verify the derived columns.
	src.GroupID = "GR12345678987654321337"
	src.InvocationUUID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	inv := ToInvocationFromPrimaryDB(src)
	all := ToAllInvocationFromPrimaryDB(src)

	// Every field AllInvocation shares with Invocation should match the
	// Invocation conversion exactly.
	invType := reflect.TypeOf(*inv)
	invValue := reflect.ValueOf(*inv)
	allValue := reflect.ValueOf(*all)
	for _, f := range reflect.VisibleFields(invType) {
		destField := allValue.FieldByName(f.Name)
		require.True(t, destField.IsValid(), "AllInvocation is missing shared field %q", f.Name)
		assert.Equal(t, invValue.FieldByName(f.Name).Interface(), destField.Interface(),
			"shared field %q differs between Invocation and AllInvocation conversions", f.Name)
	}

	// small_group_id = toUInt64(substring("GR12345678987654321337", 3))
	assert.Equal(t, uint64(12345678987654321337), all.SmallGroupID)
	// small_inv_uuid = reinterpretAsUInt128(unhex(...)) reads the 16 bytes
	// little-endian, so the big.Int is built from the reversed bytes.
	expectedUUID := new(big.Int).SetBytes([]byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0})
	require.NotNil(t, all.SmallInvUUID)
	assert.Equal(t, 0, expectedUUID.Cmp(all.SmallInvUUID), "small_inv_uuid mismatch: got %s, want %s", all.SmallInvUUID, expectedUUID)
}

func TestSmallGroupID(t *testing.T) {
	assert.Equal(t, uint64(0), smallGroupID(""))
	assert.Equal(t, uint64(0), smallGroupID("GR"))
	assert.Equal(t, uint64(123), smallGroupID("GR123"))
	assert.Equal(t, uint64(12345678987654321337), smallGroupID("GR12345678987654321337"))
	// Non-numeric suffix can't be parsed; falls back to 0.
	assert.Equal(t, uint64(0), smallGroupID("GRabc"))
}

func TestExtractProjectionNames(t *testing.T) {
	createStmt := `
CREATE TABLE bb_test_target.TestTargetStatuses
(
` +
		"`group_id` String," +
		"`repo_url` String," +
		`
    PROJECTION projection_commits
    (
        SELECT
            group_id,
            role,
            repo_url,
            commit_sha,
            max(created_at_usec) AS latest_created_at_usec
        GROUP BY
            group_id,
            role,
            repo_url,
            commit_sha
    )
	PROJECTION projection_targets
	( 
	    SELECT 
		    group_id,
			role,
			repo_url,
			label
	)
)
ENGINE = ReplacingMergeTree
ORDER BY (group_id, repo_url)
SETTINGS index_granularity = 8192
`
	projectionNames := extractProjectionNamesFromCreateStmt(createStmt)
	assert.Contains(t, projectionNames, "projection_commits")
	assert.Contains(t, projectionNames, "projection_targets")

}

func TestUsageViewQuery(t *testing.T) {
	const expectedQuery = "SELECT group_id, period_start, sku, labels, SUM(count) AS count " +
		"FROM RawUsage FINAL GROUP BY group_id, period_start, sku, labels"

	// RawUsage may contain duplicate flushes from Redis buffers, so the view
	// deduplicates with FINAL before aggregating counts by Usage dimensions.
	assert.Equal(t, expectedQuery, (&Usage{}).ViewQuery())
}

func TestSameViewQuery(t *testing.T) {
	// ClickHouse may store the saved SELECT with different formatting and
	// function casing, so avoid replacing the view when the query is
	// semantically the same for this generated view.
	assert.True(t, sameViewQuery(
		"SELECT `group_id`, `period_start`, `sku`, `labels`, sum(`count`) AS `count` FROM `db_name`.`RawUsage` FINAL GROUP BY `group_id`, `period_start`, `sku`, `labels`",
		"SELECT group_id, period_start, sku, labels, SUM(count) AS count FROM RawUsage FINAL GROUP BY group_id, period_start, sku, labels",
		"db_name",
	))

	// A different grouping or aggregate expression should trigger replacement.
	assert.False(t, sameViewQuery(
		"SELECT group_id, period_start, sku, SUM(count) AS count FROM RawUsage FINAL GROUP BY group_id, period_start, sku",
		"SELECT group_id, period_start, sku, labels, SUM(count) AS count FROM RawUsage FINAL GROUP BY group_id, period_start, sku, labels",
		"",
	))
}
