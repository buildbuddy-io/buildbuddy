package schema

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-faker/faker/v4"
)

func isInList(fieldName string, fields []string) bool {
	for _, fn := range fields {
		if fn == fieldName {
			return true
		}
	}
	return false
}

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
			clickhouseTable: &Execution{},
			primaryDBTable:  tables.Execution{},
		},
		{
			clickhouseTable: &TestTargetStatus{},
			// don't testing schema in sync
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
				isIncluded := isInList(chField.Name, additionalFields)
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
				isExcluded := isInList(primaryField.Name, excludedFields)
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

	for _, primaryField := range primaryInvFields {
		if isInList(primaryField.Name, excludedFields) || primaryField.Anonymous || primaryField.Name == "InvocationUUID" {
			// check InvocationUUID field seperately
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
