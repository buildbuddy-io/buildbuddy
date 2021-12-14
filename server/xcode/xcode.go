//go:build !darwin
// +build !darwin

package xcode

import "context"

type xcodeLocator struct {
}

func NewXcodeLocator(ctx context.Context) (*xcodeLocator, error) {
	return &xcodeLocator{}, nil
}

func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	return "", nil
}

func (x *xcodeLocator) IsSDKPathPresentForVersion(sdkPath, version string) bool {
	return false
}
