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

func TestXcodeSDKs(t *testing.T) {
	developerDir := "/Applications/Xcode.app/Contents/Developer"
	sdkPaths := []string{
		"Platforms/AppleTVOS.platform/Developer/SDKs/AppleTVOS.sdk",
		"Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk",
		"Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.2.sdk",
		"Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.sdk",
		"Platforms/WatchOS.platform/Developer/SDKs/WatchOS.sdk",
		"Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator.sdk",
		"Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator18.2.sdk",
	}
	xcrunPaths := map[string]string{
		"MacOSX":          developerDir + "/Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.2.sdk",
		"WatchOS":         "/Library/Developer/SDKs/WatchOS.sdk",
		"iPhoneSimulator": developerDir + "/Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator18.2.sdk",
	}
	showSDKPath := func(dir, sdk string) (string, error) {
		require.Equal(t, developerDir, dir)
		if path, ok := xcrunPaths[sdk]; ok {
			return path, nil
		}
		return "", fmt.Errorf("xcrun cannot find SDK %s", sdk)
	}

	require.Equal(t, map[string]string{
		// xcrun failed, so the plain path stays.
		"AppleTVOS":  "Platforms/AppleTVOS.platform/Developer/SDKs/AppleTVOS.sdk",
		"MacOSX":     "Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.2.sdk",
		"MacOSX15.2": "Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.2.sdk",
		"MacOSX15":   "Platforms/MacOSX.platform/Developer/SDKs/MacOSX15.sdk",
		// xcrun returned a path outside the developer dir, so the plain path stays.
		"WatchOS":             "Platforms/WatchOS.platform/Developer/SDKs/WatchOS.sdk",
		"iPhoneSimulator":     "Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator18.2.sdk",
		"iPhoneSimulator18.2": "Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator18.2.sdk",
	}, xcodeSDKs(developerDir, sdkPaths, showSDKPath))
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
