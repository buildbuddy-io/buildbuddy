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
	chInvType := reflect.TypeOf(clickhouse.Invocation{})
	primaryInvType := reflect.TypeOf(tables.Invocation{})

	chInvFields := reflect.VisibleFields(chInvType)
	primaryInvFields := reflect.VisibleFields(primaryInvType)

	for _, chField := range chInvFields {
		_, found := primaryInvType.FieldByName(chField.Name)
		assert.True(t, found, "Field %q is not found in tables.Invocation", chField.Name)
	}

	excludedFields := (&clickhouse.Invocation{}).ExcludedFields()
	for _, primaryField := range primaryInvFields {
		if primaryField.Anonymous {
			continue
		}

		_, found := chInvType.FieldByName(primaryField.Name)
		isExcluded := isExcluded(primaryField.Name, excludedFields)
		if isExcluded {
			assert.False(t, found, "Field %q is found in clickhouse.Invocation, but it's marked as excluded", primaryField.Name)
		} else {
			assert.True(t, found, "Field %q is not found in clickhouse.Invocation", primaryField.Name)
		}
		assert.NotEqual(t, found, isExcluded, "Field %q is not found in clickhouse.Invocation", primaryField.Name)
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
