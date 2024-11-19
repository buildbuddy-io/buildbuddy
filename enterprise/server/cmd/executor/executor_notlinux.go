//go:build !linux

package main

import "context"

func getActionsCgroupParent() (string, error) {
	return "", nil
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}
