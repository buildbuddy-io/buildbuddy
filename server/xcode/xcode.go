//go:build !darwin
// +build !darwin

package xcode

import "context"

type xcodeLocator struct {
}

func NewXcodeLocator(context.Context) *xcodeLocator {
	return &xcodeLocator{}
}

func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	return "", nil
}

func (x *xcodeLocator) IsSDKPathPresentForVersion(sdkPath, version string) bool {
	return false
}
