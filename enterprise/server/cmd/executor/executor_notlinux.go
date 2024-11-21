//go:build !linux

package main

import "context"

func setupCgroups() (string, error) {
	return "", nil
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}
