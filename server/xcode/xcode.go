//go:build !darwin
// +build !darwin

package xcode

type xcodeLocator struct {
}

func NewXcodeLocator() *xcodeLocator {
	return &xcodeLocator{}
}

func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	return "", nil
}

func (x *xcodeLocator) PathsForVersionAndSDK(xcodeVersion string, sdk string) (string, string, error) {
	return "", "", nil
}
