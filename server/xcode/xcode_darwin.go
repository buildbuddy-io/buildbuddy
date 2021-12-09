//go:build darwin && !ios
// +build darwin,!ios

package xcode

/*
#cgo LDFLAGS: -framework CoreServices
#import <CoreServices/CoreServices.h>
*/
import "C"

import (
	"io/fs"
	"os"
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
	sdks             []string
}

func NewXcodeLocator() (*xcodeLocator, error) {
	xl := &xcodeLocator{}
	err := xl.locate()
	return xl, err
}

// Finds the XCode developer directory that most closely matches the given XCode version.
func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	xv := x.xcodeVersionForVersionString(version)
	if xv == nil {
		return "", status.FailedPreconditionErrorf("XCode version %s not installed on remote executor. Available Xcode versions are %+v", version, x.versions)
	}
	return xv.developerDirPath, nil
}

// Return true if the given SDK path is present in the given XCode version.
func (x *xcodeLocator) IsSDKPathPresentForVersion(sdkPath, version string) bool {
	xv := x.xcodeVersionForVersionString(version)
	if xv == nil {
		return false
	}
	for _, sdk := range xv.sdks {
		if sdk == sdkPath {
			return true
		}
	}
	return false
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

// Locates all all XCode versions installed on the host machine.
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

	versionMap := make(map[string]*xcodeVersion)
	for _, urlRef := range urlRefs {
		path := "/" + strings.TrimLeft(stringFromCFString(C.CFURLGetString(C.CFURLRef(urlRef))), filePrefix)
		versionFileReader, err := os.Open(path + versionPlistPath)
		if err != nil {
			log.Warningf("Error reading version file for XCode located at %s: %s", path, err.Error())
			continue
		}
		// The interesting bits to pull from XCode's version plist.
		var xcodePlist struct {
			CFBundleShortVersionString string `plist:"CFBundleShortVersionString"`
			ProductBuildVersion        string `plist:"ProductBuildVersion"`
		}
		if err := plist.NewXMLDecoder(versionFileReader).Decode(&xcodePlist); err != nil {
			log.Warningf("Error decoding plist for XCode located at %s: %s", path, err.Error())
			continue
		}
		developerDirPath := path + developerDirectoryPath
		sdks, err := fs.Glob(os.DirFS(developerDirPath), "Platforms/*.platform/Developer/SDKs/*")
		if err != nil {
			log.Warningf("Error reading XCode SDKs from %s: %s", path, err.Error())
			continue
		}
		log.Infof("Found XCode version %s.%s at path %s", xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion, path)
		versions := expandXCodeVersions(xcodePlist.CFBundleShortVersionString, xcodePlist.ProductBuildVersion)
		mostPreciseVersion := versions[len(versions)-1]
		for _, version := range versions {
			existingXcode, ok := versionMap[version]
			if ok && mostPreciseVersion < existingXcode.version {
				continue
			}
			log.Debugf("Mapped XCode Version %s=>%s with SDKs %+v", version, developerDirPath, sdks)
			versionMap[version] = &xcodeVersion{
				version:          mostPreciseVersion,
				developerDirPath: developerDirPath,
				sdks:             sdks,
			}
		}
	}
	x.versions = versionMap
	return nil
}

// Expands a single XCode version into all component combinations in order of increasing precision.
// i.e. 12.5 => 12, 12.5, 12.5.0, 12.5.0.abc123
func expandXCodeVersions(xcodeVersion string, productVersion string) []string {
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
