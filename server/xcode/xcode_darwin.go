//go:build darwin && !ios
// +build darwin,!ios

package xcode

/*
#cgo LDFLAGS: -framework CoreServices
#import <CoreServices/CoreServices.h>
*/
import "C"

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/groob/plist"
)

const xcodeBundleID = "com.apple.dt.Xcode"
const versionPlistPath = "Contents/version.plist"
const developerDirectoryPath = "Contents/Developer"
const filePrefix = "file://"

type xcodeLocator struct {
	versions map[string]*xcodeVersion
}

type xcodeVersion struct {
	version          string
	developerDirPath string
	sdks             map[string]string
}

// The interesting bits to pull from Xcode's version plist.
type xcodePlist struct {
	CFBundleShortVersionString string `plist:"CFBundleShortVersionString"`
	ProductBuildVersion        string `plist:"ProductBuildVersion"`
}

func NewXcodeLocator() (*xcodeLocator, error) {
	xl := &xcodeLocator{}
	err := xl.locate()
	return xl, err
}

// Finds the Xcode that matches the given Xcode version.
// Returns the developer directory for that Xcode and the SDK root for the given SDK.
func (x *xcodeLocator) PathsForVersionAndSDK(xcodeVersion string, sdk string) (string, string, error) {
	xv := x.xcodeVersionForVersionString(xcodeVersion)
	if xv == nil {

		return "", "", status.FailedPreconditionErrorf("Xcode version %s not installed on remote executor. Available Xcode versions are %s", xcodeVersion, versionsString(x.versions))
	}
	sdkPath, ok := xv.sdks[sdk]
	if !ok {
		return "", "", status.FailedPreconditionErrorf("SDK %s not available for Xcode %s. Available SDKs are %s", sdk, xcodeVersion, sdksString(xv.sdks))
	}
	sdkRoot := fmt.Sprintf("%s/%s", xv.developerDirPath, sdkPath)
	return xv.developerDirPath, sdkRoot, nil
}

// Returns the xcodeVersion most closely matching the version string.
func (x *xcodeLocator) xcodeVersionForVersionString(version string) *xcodeVersion {
	versionComponents := strings.Split(version, ".")
	for i := range versionComponents {
		subVersion := strings.Join(versionComponents[0:len(versionComponents)-i], ".")
		if xcodeVersion, ok := x.versions[subVersion]; ok {
			return xcodeVersion
		}
	}
	return nil
}

// Locates all Xcode versions installed on the host machine.
// Very losely based on https://github.com/bazelbuild/bazel/blob/master/tools/osx/xcode_locator.m
func (x *xcodeLocator) locate() error {
	bundleID := stringToCFString(xcodeBundleID)
	defer C.CFRelease(C.CFTypeRef(bundleID))

	urlsPointer := C.LSCopyApplicationURLsForBundleIdentifier(bundleID, nil)

	if urlsPointer == C.CFArrayRef(unsafe.Pointer(nil)) {
		return status.FailedPreconditionErrorf("cannot find Xcode bundle: %q. Make sure Xcode is installed.", xcodeBundleID)
	}
	defer C.CFRelease(C.CFTypeRef(urlsPointer))

	urlsArrayRef := C.CFArrayRef(urlsPointer)
	n := C.CFArrayGetCount(urlsArrayRef)

	urlRefs := make([]C.CFURLRef, n)
	C.CFArrayGetValues(urlsArrayRef, C.CFRange{0, n}, (*unsafe.Pointer)(unsafe.Pointer(&urlRefs[0])))

	x.versions = versionMap(urlRefs)
	return nil
}

func versionMap(urlRefs []C.CFURLRef) map[string]*xcodeVersion {
	versionMap := make(map[string]*xcodeVersion)
	for _, urlRef := range urlRefs {
		path := "/" + strings.TrimLeft(stringFromCFString(C.CFURLGetString(C.CFURLRef(urlRef))), filePrefix)
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
		sdks := make(map[string]string)
		for _, sdkPath := range sdkPaths {
			sdks[strings.TrimSuffix(filepath.Base(sdkPath), ".sdk")] = sdkPath
		}
		log.Infof("Found Xcode version %s (%s) at path %s", xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion, path)
		versions := expandXcodeVersions(xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion)
		mostPreciseVersion := versions[len(versions)-1]
		for _, version := range versions {
			existingXcode, ok := versionMap[version]
			if ok && mostPreciseVersion < existingXcode.version {
				continue
			}
			log.Debugf("Mapped Xcode Version %s=>%s with SDKs %+v", version, developerDirPath, sdks)
			versionMap[version] = &xcodeVersion{
				version:          mostPreciseVersion,
				developerDirPath: developerDirPath,
				sdks:             sdks,
			}
		}
	}
	return versionMap
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
	for sdk, _ := range sdks {
		availableSDKs = append(availableSDKs, sdk)
	}
	sort.Strings(availableSDKs)
	return strings.Join(availableSDKs, ", ")
}

func versionsString(versions map[string]*xcodeVersion) string {
	availableVersions := make([]string, 0, len(versions))
	for version, _ := range versions {
		availableVersions = append(availableVersions, version)
	}
	sort.Strings(availableVersions)
	return strings.Join(availableVersions, ", ")
}
