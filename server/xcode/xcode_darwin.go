//go:build darwin && !ios

package xcode

/*
#cgo LDFLAGS: -framework CoreServices
#import <CoreServices/CoreServices.h>
*/
import "C"

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/groob/plist"
)

const xcodeBundleID = "com.apple.dt.Xcode"
const versionPlistPath = "Contents/version.plist"
const developerDirectoryPath = "Contents/Developer"
const filePrefix = "file://"
const defaultXcodeVersion = "default-xcode-version"

var desiredXcodeVersions = flag.Slice("executor.desired_xcode_versions", []string{}, "List of Xcode versions desired on the host system. If any of the provided Xcode versions cannot be found on the host, the executor will log a warning message.")

type xcodeLocator struct {
	// Map from Xcode version string (e.g. "16" or "16.0.0.16A242") to an
	// xcodeVersion containing information about that Xcode installation. Note
	// that a single xcodeVersion will appear multiple times in this map, once
	// for each degree of version specificity. For example, Xcode version
	// "16.0.0" will appear under the following keys: "16", "16.0", "16.0.0",
	// and "16.0.0.16A242" (16A242 is the build number). This is done so that
	// the client may target it via just the major version number, or a more
	// specific version if desired.
	versions map[string]*xcodeVersion
}

type xcodeVersion struct {
	// The specific version of Xcode (e.g. "16.0.0.16A242")
	version          string
	developerDirPath string

	// This map contains all of the Xcode SDKs that are installed and available
	// for this version of Xcode. The keys are the SDK versions, and the values
	// are the absolute paths used as SDKROOT. Like the
	// xcodeLocator.versions map above, SDKs can appear in this map multiple
	// times under different levels of version specifity, for example this map
	// can contain keys: "iPhoneSimulator" and "iPhoneSimulator18.2" pointing
	// to the same underlying SDK.
	sdks map[string]string
}

// The interesting bits to pull from Xcode's version plist.
type xcodePlist struct {
	CFBundleShortVersionString string `plist:"CFBundleShortVersionString"`
	ProductBuildVersion        string `plist:"ProductBuildVersion"`
}

func NewXcodeLocator() *xcodeLocator {
	xl := &xcodeLocator{}
	xl.locate()
	xl.verify()
	return xl
}

// Checks the list of desired xcode versions, logging a warning if any of them
// are not present on the host system.
func (x *xcodeLocator) verify() {
	for _, version := range *desiredXcodeVersions {
		if _, ok := x.versions[version]; !ok {
			alert.UnexpectedEvent("xcode version missing", "Failed to locate desired Xcode version %s", version)
		}
	}
}

func (x *xcodeLocator) Versions() []string {
	xcodes := map[string]struct{}{}
	for _, xcode := range x.versions {
		if xcode.version == defaultXcodeVersion {
			continue
		}
		xcodes[xcode.version] = struct{}{}
	}
	return slices.Sorted(maps.Keys(xcodes))
}

// Finds the Xcode that matches the given Xcode version.
// Returns the developer directory for that Xcode and the SDK root for the given SDK.
func (x *xcodeLocator) PathsForVersionAndSDK(xcodeVersion string, sdk string) (string, string, error) {
	if xcodeVersion == "" {
		xcodeVersion = defaultXcodeVersion
	}

	xv, ok := x.versions[xcodeVersion]
	if !ok {
		var err error
		if xcodeVersion == defaultXcodeVersion {
			err = status.FailedPreconditionErrorf("Default Xcode version not set on remote executor and xcode-select set to an invalid path. Available Xcode versions are %s", versionsString(x.versions))
		} else {
			err = status.FailedPreconditionErrorf("Xcode version %s not installed on remote executor. Available Xcode versions are %s", xcodeVersion, versionsString(x.versions))
		}
		return "", "", err
	}
	sdkRoot, ok := xv.sdks[sdk]
	if !ok {
		return "", "", status.FailedPreconditionErrorf("SDK %s not available for Xcode %s. Available SDKs are %s", sdk, xcodeVersion, sdksString(xv.sdks))
	}
	return xv.developerDirPath, sdkRoot, nil
}

// Locates all Xcode versions installed on the host machine.
// Very losely based on https://github.com/bazelbuild/bazel/blob/master/tools/osx/xcode_locator.m
func (x *xcodeLocator) locate() {
	x.versions = versionMap(urlRefs(xcodeBundleID))
}

func urlRefs(bundleID string) []C.CFURLRef {
	cfBundleID := stringToCFString(bundleID)
	defer C.CFRelease(C.CFTypeRef(cfBundleID))

	urlsPointer := C.LSCopyApplicationURLsForBundleIdentifier(cfBundleID, nil)

	if urlsPointer == C.CFArrayRef(unsafe.Pointer(nil)) {
		return []C.CFURLRef{}
	}
	defer C.CFRelease(C.CFTypeRef(urlsPointer))

	urlsArrayRef := C.CFArrayRef(urlsPointer)
	n := C.CFArrayGetCount(urlsArrayRef)

	refs := make([]C.CFURLRef, n)
	C.CFArrayGetValues(urlsArrayRef, C.CFRange{0, n}, (*unsafe.Pointer)(unsafe.Pointer(&refs[0])))

	return refs
}

func versionMap(urlRefs []C.CFURLRef) map[string]*xcodeVersion {
	defaultDeveloperDir := xcodeSelectDeveloperDir()
	versionMap := make(map[string]*xcodeVersion)
	for _, urlRef := range urlRefs {
		path := strings.TrimPrefix(stringFromCFString(C.CFURLGetString(C.CFURLRef(urlRef))), filePrefix)
		xcodePlist, err := xcodePlistForPath(path + versionPlistPath)
		if err != nil {
			log.Warningf("Error reading plist for Xcode: %s", err.Error())
			continue
		}
		developerDirPath := path + developerDirectoryPath
		sdkPaths, err := fs.Glob(os.DirFS(developerDirPath), "Platforms/*.platform/Developer/SDKs/*")
		if err != nil {
			log.Warningf("Error reading Xcode SDKs from %s: %s", path, err.Error())
			continue
		}
		sdks := xcodeSDKs(developerDirPath, sdkPaths, xcrunSDKPath)
		log.Infof("Found Xcode version %s (%s) at path %s", xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion, path)
		versions := expandXcodeVersions(xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion)
		mostPreciseVersion := versions[len(versions)-1]
		for _, version := range versions {
			existingXcode, ok := versionMap[version]
			if ok && mostPreciseVersion < existingXcode.version {
				continue
			}
			log.Debugf("Mapped Xcode Version %s=>%s with SDKs %+v", version, developerDirPath, sdks)
			xv := &xcodeVersion{
				version:          mostPreciseVersion,
				developerDirPath: developerDirPath,
				sdks:             sdks,
			}
			versionMap[version] = xv
			if defaultDeveloperDir == developerDirPath {
				versionMap[defaultXcodeVersion] = xv
			}
		}
	}

	if len(versionMap) == 0 {
		log.Warningf("Could not find any installed Xcode apps")
	} else if xv, ok := versionMap[defaultXcodeVersion]; ok {
		log.Infof("xcode-select set to Xcode version %s", xv.version)
		return versionMap
	}

	// If we are here then defaultDeveloperDir is not a valid Xcode path
	if defaultDeveloperDir == "/Library/Developer/CommandLineTools" {
		version := commandLineToolsVersionString()
		log.Infof("xcode-select set to Command Line Tools (%s)", version)
		if xv := commandLineToolsVersion(defaultDeveloperDir); xv != nil {
			versionMap[defaultXcodeVersion] = xv
		}
	} else if defaultDeveloperDir != "" {
		log.Warningf("xcode-select set to an invalid Xcode path: %s", defaultDeveloperDir)
	}

	return versionMap
}

// Maps SDK names like "iPhoneSimulator" and "iPhoneSimulator18.2" to the
// absolute paths used as SDKROOT. sdkPaths are relative to developerDirPath.
func xcodeSDKs(developerDirPath string, sdkPaths []string, showSDKPath func(developerDir, sdk string) (string, error)) map[string]string {
	sdks := make(map[string]string)
	for _, sdkPath := range sdkPaths {
		sdks[strings.TrimSuffix(filepath.Base(sdkPath), ".sdk")] = developerDirPath + "/" + sdkPath
	}
	// Bazel sets SDKROOT for local actions to the path xcrun reports. For an
	// Xcode SDK that is the versioned symlink, e.g. iPhoneSimulator18.2.sdk.
	// Use the same path so local and remote actions get the same SDKROOT.
	for sdk, sdkRoot := range sdks {
		if sdkRoot != fmt.Sprintf("%s/Platforms/%s.platform/Developer/SDKs/%s.sdk", developerDirPath, sdk, sdk) {
			continue
		}
		xcrunSDKRoot, err := showSDKPath(developerDirPath, sdk)
		if err != nil {
			log.Warningf("Error finding SDK %s in %s with xcrun: %s", sdk, developerDirPath, err)
			continue
		}
		sdks[sdk] = xcrunSDKRoot
	}
	return sdks
}

// Runs the same command as Bazel's XcodeLocalEnvProvider.
func xcrunSDKPath(developerDir string, sdk string) (string, error) {
	cmd := exec.Command("/usr/bin/xcrun", "--sdk", strings.ToLower(sdk), "--show-sdk-path")
	cmd.Env = []string{"DEVELOPER_DIR=" + developerDir}
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func xcodePlistForPath(path string) (*xcodePlist, error) {
	var xcodePlist xcodePlist
	versionFileReader, err := os.Open(path)
	if err != nil {
		return nil, status.InternalErrorf("Failed to open %s: %s", path, err.Error())
	}
	if err := plist.NewXMLDecoder(versionFileReader).Decode(&xcodePlist); err != nil {
		return nil, status.InternalErrorf("Failed to decode %s: %s", path, err.Error())
	}
	return &xcodePlist, nil
}

func commandLineToolsVersion(developerDir string) *xcodeVersion {
	sdkPaths, err := fs.Glob(os.DirFS(developerDir), "SDKs/*")
	if err != nil {
		log.Warningf("Error reading CommandLineTools SDKs from %s: %s", developerDir, err.Error())
		return nil
	}
	return &xcodeVersion{
		version:          defaultXcodeVersion,
		developerDirPath: developerDir,
		sdks:             xcodeSDKs(developerDir, sdkPaths, xcrunSDKPath),
	}
}

// Returns the version of the Xcode Command Line Tools, or "unknown" if it
// can't be determined.
func commandLineToolsVersionString() string {
	out, err := exec.Command(
		"sh", "-c",
		"pkgutil --pkg-info=com.apple.pkg.CLTools_Executables | awk '/version:/ {print $2}'",
	).Output()
	if err != nil {
		log.Warningf("Error determining Xcode Command Line Tools version: %s", err.Error())
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// Returns the active developer directory according to xcode-select, or the
// empty string if xcode-select failed to run.
func xcodeSelectDeveloperDir() string {
	out, err := exec.Command("xcode-select", "-p").Output()
	if err != nil {
		log.Warningf("Error calling xcode-select: %s", err.Error())
		return ""
	}
	return strings.TrimSpace(string(out))
}

// Expands a single Xcode version into all component combinations in order of increasing precision.
// i.e. 12.5 => 12, 12.5, 12.5.0, 12.5.0.abc123
func expandXcodeVersions(xcodeVersion string, productVersion string) []string {
	versions := make([]string, 0)

	components := strings.Split(xcodeVersion, ".")
	for len(components) < 3 {
		components = append(components, "0")
	}

	versions = append(versions, components[0])
	versions = append(versions, components[0]+"."+components[1])
	versions = append(versions, components[0]+"."+components[1]+"."+components[2])
	versions = append(versions, components[0]+"."+components[1]+"."+components[2]+"."+productVersion)

	return versions
}

// Converts a go string into a CFStringRef.
func stringToCFString(gostr string) C.CFStringRef {
	cstr := C.CString(gostr)
	defer C.free(unsafe.Pointer(cstr))

	return C.CFStringCreateWithCString(0, cstr, C.kCFStringEncodingUTF8)
}

// Converts a CFStringRef into a go string.
func stringFromCFString(cfStr C.CFStringRef) string {
	return C.GoString(C.CFStringGetCStringPtr(cfStr, C.kCFStringEncodingUTF8))
}

func sdksString(sdks map[string]string) string {
	availableSDKs := make([]string, 0, len(sdks))
	for sdk := range sdks {
		availableSDKs = append(availableSDKs, sdk)
	}
	sort.Strings(availableSDKs)
	return strings.Join(availableSDKs, ", ")
}

func versionsString(versions map[string]*xcodeVersion) string {
	availableVersions := make([]string, 0, len(versions))
	for version := range versions {
		if version == defaultXcodeVersion {
			continue
		}
		availableVersions = append(availableVersions, version)
	}
	sort.Strings(availableVersions)
	return strings.Join(availableVersions, ", ")
}
