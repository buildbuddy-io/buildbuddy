package clickhouse_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-faker/faker/v4"
)

func isExcluded(fieldName string, excludedFields []string) bool {
	for _, fn := range excludedFields {
		if fn == fieldName {
			return true
		}
	}
	return false
}

func TestSchemaInSync(t *testing.T) {
	tests := []struct {
		clickhouseTable clickhouse.Table
		primaryDBTable  interface{}
	}{
		{
			clickhouseTable: &clickhouse.Invocation{},
			primaryDBTable:  tables.Invocation{},
		},
	}

	for _, tc := range tests {

		chType := reflect.Indirect(reflect.ValueOf(tc.clickhouseTable)).Type()

		t.Run(chType.Name(), func(t *testing.T) {
			primaryDBType := reflect.TypeOf(tc.primaryDBTable)

			chFields := reflect.VisibleFields(chType)
			primaryFields := reflect.VisibleFields(primaryDBType)

			for _, chField := range chFields {
				_, found := primaryDBType.FieldByName(chField.Name)
				assert.True(t, found, "Field %q is not found in %s", chField.Name, primaryDBType)
			}

			excludedFields := tc.clickhouseTable.ExcludedFields()
			for _, primaryField := range primaryFields {
				if primaryField.Anonymous {
					continue
				}

				_, found := chType.FieldByName(primaryField.Name)
				isExcluded := isExcluded(primaryField.Name, excludedFields)
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
	dest := clickhouse.ToInvocationFromPrimaryDB(src)

	primaryInvType := reflect.TypeOf(*src)
	primaryInvFields := reflect.VisibleFields(primaryInvType)
	srcValue := reflect.ValueOf(*src)
	destValue := reflect.ValueOf(*dest)
	excludedFields := (&clickhouse.Invocation{}).ExcludedFields()

	expectedUUID := hex.EncodeToString(src.InvocationUUID)
	assert.Equal(t, dest.InvocationUUID, expectedUUID, "src and dest have different values for field 'InvocationUUID'. src = %v, dest = %v", src.InvocationUUID, expectedUUID)

	for _, primaryField := range primaryInvFields {
		if isExcluded(primaryField.Name, excludedFields) || primaryField.Anonymous || primaryField.Name == "InvocationUUID" {
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
