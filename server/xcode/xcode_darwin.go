// +build darwin
// +build !ios

package xcode

/*
#cgo LDFLAGS: -framework CoreServices
#import <CoreServices/CoreServices.h>
*/
import "C"

import (
	"os"
	"strings"
	"unicode/utf16"
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
	versions map[string]string
}

func NewXcodeLocator() *xcodeLocator {
	return &xcodeLocator{}
}

// Finds the XCode developer directory that most closely matches the given XCode version.
func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	versionComponents := strings.Split(version, ".")
	for i, _ := range versionComponents {
		subVersion := strings.Join(versionComponents[0:len(versionComponents)-i], ".")
		if path, ok := x.versions[subVersion]; ok {
			return path, nil
		}
	}
	return "", status.FailedPreconditionErrorf("XCode version %s not installed on remote executor. Available Xcode versions are %+v", version, x.versions)
}

// Locates all all XCode versions installed on the host machine.
// Very losely based on https://github.com/bazelbuild/bazel/blob/master/tools/osx/xcode_locator.m
func (x *xcodeLocator) Locate() {
	bundleID := stringToCFString(xcodeBundleID)
	defer C.CFRelease(C.CFTypeRef(bundleID))

	urlsPointer := C.LSCopyApplicationURLsForBundleIdentifier(bundleID, nil)
	defer C.CFRelease(C.CFTypeRef(urlsPointer))

	urlsArrayRef := C.CFArrayRef(urlsPointer)
	n := C.CFArrayGetCount(urlsArrayRef)

	urlRefs := make([]C.CFURLRef, n)
	C.CFArrayGetValues(urlsArrayRef, C.CFRange{0, n}, (*unsafe.Pointer)(unsafe.Pointer(&urlRefs[0])))

	versionMap := make(map[string]string)
	for _, urlRef := range urlRefs {
		path := "/" + strings.TrimLeft(stringFromCFString(C.CFURLGetString(C.CFURLRef(urlRef))), filePrefix)

		versionFileReader, err := os.Open(path + versionPlistPath)
		if err != nil {
			log.Warningf("Error reading version file for XCode located at %s: %s", path, err.Error())
			continue
		}

		// The interesting bits to pull from XCode's version plist.
		var xcodeVersion struct {
			CFBundleShortVersionString string `plist:"CFBundleShortVersionString"`
			ProductBuildVersion        string `plist:"ProductBuildVersion"`
		}
		if err := plist.NewXMLDecoder(versionFileReader).Decode(&xcodeVersion); err != nil {
			log.Warningf("Error decoding plist for XCode located at %s: %s", path, err.Error())
			continue
		}

		versions := expandXCodeVersions(xcodeVersion.CFBundleShortVersionString, xcodeVersion.ProductBuildVersion)
		for _, version := range versions {
			existingVersion, ok := versionMap[version]
			if ok && version < existingVersion {
				log.Debugf("Error decoding plist for XCode located at %s: %s", path, err.Error())
				continue
			}
			versionMap[version] = path + developerDirectoryPath
		}
	}

	x.versions = versionMap

	log.Infof("Found XCode versions: %+v", x.versions)
}

// Expands a single XCode version into all component combinations.
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
func stringFromCFString(s C.CFStringRef) string {
	ptr := C.CFStringGetCStringPtr(s, C.kCFStringEncodingUTF8)
	if ptr != nil {
		return C.GoString(ptr)
	}
	length := uint32(C.CFStringGetLength(s))
	uniPtr := C.CFStringGetCharactersPtr(s)
	if uniPtr == nil || length == 0 {
		return ""
	}
	return stringFromUint16Ptr((*uint16)(uniPtr), length)
}

// Converts a *uint64 into a go string.
func stringFromUint16Ptr(p *uint16, length uint32) string {
	r := []uint16{}
	ptr := uintptr(unsafe.Pointer(p))
	for i := uint32(0); i < length; i++ {
		c := *(*uint16)(unsafe.Pointer(ptr))
		r = append(r, c)
		if c == 0 {
			break
		}
		ptr = ptr + unsafe.Sizeof(c)
	}
	r = append(r, uint16(0))
	decoded := utf16.Decode(r)
	n := 0
	for i, r := range decoded {
		if r == rune(0) {
			n = i
			break
		}
	}
	return string(decoded[:n])
}
