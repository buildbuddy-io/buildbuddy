//go:build darwin && !ios

package xcode

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXcodeDiscoverySkipsUnusablePaths(t *testing.T) {
	root := t.TempDir()
	xcode := filepath.Join(root, "Xcode.app")
	developerDir := filepath.Join(xcode, developerDirectoryPath)
	sdkDir := filepath.Join(developerDir, "Platforms/MacOSX.platform/Developer/SDKs")
	writeVersionPlist(t, xcode, "26.1", "17B55")
	require.NoError(t, os.MkdirAll(filepath.Join(sdkDir, "MacOSX26.1.sdk"), 0755))
	require.NoError(t, os.Symlink("MacOSX26.1.sdk", filepath.Join(sdkDir, "MacOSX.sdk")))
	require.NoError(t, os.Symlink("MacOSX26.0.sdk", filepath.Join(sdkDir, "MacOSX26.sdk")))
	require.NoError(t, os.WriteFile(filepath.Join(sdkDir, "MacOSX26.2.sdk"), nil, 0644))
	require.NoError(t, os.Mkdir(filepath.Join(sdkDir, "Documentation"), 0755))
	incomplete := filepath.Join(root, "Incomplete Xcode.app")
	writeVersionPlist(t, incomplete, "26.2", "17C52")

	xl := &xcodeLocator{versions: versionMap([]string{xcode, incomplete}, developerDir)}
	require.Equal(t, []string{"26.1.0.17B55"}, xl.Versions())
	for _, sdk := range []string{"MacOSX", "MacOSX26.1"} {
		dev, sdkRoot, err := xl.PathsForVersionAndSDK("26", sdk)
		require.NoError(t, err)
		require.Equal(t, developerDir, dev)
		require.Equal(t, filepath.Join(sdkDir, sdk+".sdk"), sdkRoot)
	}
	for _, sdk := range []string{"MacOSX26", "MacOSX26.2", "Documentation"} {
		_, _, err := xl.PathsForVersionAndSDK("26", sdk)
		require.ErrorContains(t, err, "SDK "+sdk+" not available")
	}
}

func TestXcodeDefaultNotMatchedByLaunchServicesPath(t *testing.T) {
	root := t.TempDir()
	bundle := filepath.Join(root, "Xcode 26.1.app")
	developerDir := filepath.Join(bundle, developerDirectoryPath)
	sdkPath := "Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"
	writeVersionPlist(t, bundle, "26.1", "17B55")
	require.NoError(t, os.MkdirAll(filepath.Join(developerDir, sdkPath), 0755))

	t.Run("selected through a symlink", func(t *testing.T) {
		alias := filepath.Join(root, "Xcode.app")
		require.NoError(t, os.Symlink(bundle, alias))
		xl := &xcodeLocator{versions: versionMap([]string{bundle}, filepath.Join(alias, developerDirectoryPath))}
		dev, sdkRoot, err := xl.PathsForVersionAndSDK("", "MacOSX")
		require.NoError(t, err)
		require.Equal(t, developerDir, dev)
		require.Equal(t, filepath.Join(developerDir, sdkPath), sdkRoot)
	})
	t.Run("not registered", func(t *testing.T) {
		xl := &xcodeLocator{versions: versionMap(nil, developerDir)}
		dev, sdkRoot, err := xl.PathsForVersionAndSDK("", "MacOSX")
		require.NoError(t, err)
		require.Equal(t, developerDir, dev)
		require.Equal(t, filepath.Join(developerDir, sdkPath), sdkRoot)
	})
	t.Run("selected duplicate does not replace the registered build", func(t *testing.T) {
		const iosSDK = "Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk"
		require.NoError(t, os.MkdirAll(filepath.Join(developerDir, iosSDK), 0755))
		selectedBundle := filepath.Join(root, "Staged Xcode 26.1.app")
		selectedDir := filepath.Join(selectedBundle, developerDirectoryPath)
		writeVersionPlist(t, selectedBundle, "26.1", "17B55")
		require.NoError(t, os.MkdirAll(filepath.Join(selectedDir, sdkPath), 0755))

		xl := &xcodeLocator{versions: versionMap([]string{bundle}, selectedDir)}
		dev, sdkRoot, err := xl.PathsForVersionAndSDK("26.1.0.17B55", "iPhoneOS")
		require.NoError(t, err)
		require.Equal(t, developerDir, dev)
		require.Equal(t, filepath.Join(developerDir, iosSDK), sdkRoot)
		dev, sdkRoot, err = xl.PathsForVersionAndSDK("", "MacOSX")
		require.NoError(t, err)
		require.Equal(t, selectedDir, dev)
		require.Equal(t, filepath.Join(selectedDir, sdkPath), sdkRoot)
	})
}

func writeVersionPlist(t *testing.T, bundle, version, build string) {
	t.Helper()
	path := filepath.Join(bundle, versionPlistPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	contents := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleShortVersionString</key><string>%s</string>
<key>ProductBuildVersion</key><string>%s</string>
</dict></plist>`, version, build)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
}

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
