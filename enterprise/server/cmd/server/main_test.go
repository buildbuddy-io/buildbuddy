package main

import (
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlagsAgainstConfig(t *testing.T) {
	configurator, err := config.NewConfiguratorFromData([]byte{})
	require.NoError(t, err)
	yamlFlagSet := configurator.GenerateFlagSet()
	flag.VisitAll(func(flg *flag.Flag) {
		if strings.HasPrefix(flg.Name, "test.") || !strings.Contains(flg.Name, ".") {
			return
		}
		yamlFlg := yamlFlagSet.Lookup(flg.Name)
		assert.NotNil(t, yamlFlg, "Flag %s is not present in the yaml config", flg.Name)
		if yamlFlg == nil {
			return
		}
		assert.Equal(t, reflect.TypeOf(yamlFlg.Value), reflect.TypeOf(flg.Value), "Flag %s is of type %T, but yaml flag is of type %T", flg.Name, flg.Value, yamlFlg.Value)
		assert.Equal(t, yamlFlg.Usage, flg.Usage, "Flag %s is has usage: `%s`, but yaml docstring is `%s`", flg.Name, flg.Usage, yamlFlg.Usage)
	})
}
