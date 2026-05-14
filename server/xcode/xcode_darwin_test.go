//go:build darwin && !ios

package xcode

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXcodeLocator(t *testing.T) {
	xl := NewXcodeLocator()
	for k, v := range xl.versions {
		t.Run(k, func(t *testing.T) {
			require.False(
				t,
				strings.HasPrefix(v.developerDirPath, "//"),
				fmt.Sprintf("developerDirPath of Xcode version %q should not has '//' prefix: %q", k, v.developerDirPath),
			)
			require.DirExists(t, v.developerDirPath)
		})
	}
}

func TestVersions(t *testing.T) {
	xl := xcodeLocator{}
	require.Empty(t, xl.Versions())

	xcode154Version := "15.4.0.92A424"
	xcode160Version := "16.0.0.16A242"
	xcode164Version := "16.4.0.55A224"
	xcode154 := &xcodeVersion{version: xcode154Version}
	xcode160 := &xcodeVersion{version: xcode160Version}
	xcode164 := &xcodeVersion{version: xcode164Version}
	xl = xcodeLocator{
		versions: map[string]*xcodeVersion{
			"15":            xcode154,
			"15.4":          xcode154,
			"15.4.0":        xcode154,
			xcode154Version: xcode154,
			"16":            xcode164,
			"16.0":          xcode160,
			"16.0.0":        xcode160,
			xcode160Version: xcode160,
			"16.4":          xcode164,
			"16.4.0":        xcode164,
			xcode164Version: xcode164,
		},
	}
	require.Equal(t,
		[]string{xcode154Version, xcode160Version, xcode164Version},
		xl.Versions())
}

func TestSDKs(t *testing.T) {
	xl := xcodeLocator{}
	require.Empty(t, xl.SDKs())

	xcode160Version := "16.0.0.16A242"
	xcode164Version := "16.4.0.55A224"
	sdks := map[string]string{
		"AppleTVOS":            "/Users/administrator/AppleTVOS18.0",
		"AppleTVOS18.0":        "/Users/administrator/AppleTVOS18.0",
		"AppleTVSimulator":     "/Users/administrator/AppleTVSimulator18.0",
		"AppleTVSimulator18.0": "/Users/administrator/AppleTVSimulator18.0",
		"iPhoneOS":             "/Users/administrator/iPhoneOS18.0",
		"iPhoneOS18.0":         "/Users/administrator/iPhoneOS18.0",
		"iPhoneSimulator":      "/Users/administrator/iPhoneSimulator18.0",
		"iPhoneSimulator18.0":  "/Users/administrator/iPhoneSimulator18.0",
	}
	xcode160 := &xcodeVersion{version: xcode160Version, sdks: sdks}
	xcode164 := &xcodeVersion{version: xcode164Version, sdks: sdks}
	xl = xcodeLocator{
		versions: map[string]*xcodeVersion{
			"16":            xcode164,
			"16.0":          xcode160,
			"16.0.0":        xcode160,
			xcode160Version: xcode160,
			"16.4":          xcode164,
			"16.4.0":        xcode164,
			xcode164Version: xcode164,
		},
	}
	require.Equal(t,
		[]string{
			"AppleTVOS18.0",
			"AppleTVSimulator18.0",
			"iPhoneOS18.0",
			"iPhoneSimulator18.0"},
		xl.SDKs())
}
