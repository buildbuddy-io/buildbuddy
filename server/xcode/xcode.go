//go:build !darwin
// +build !darwin

package xcode

type xcodeLocator struct {
}

func NewXcodeLocator() (*xcodeLocator, error) {
	return &xcodeLocator{}, nil
}

func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	return "", nil
}

func (x *xcodeLocator) IsSDKPathPresentForVersion(sdkPath, version string) bool {
	return false
}
