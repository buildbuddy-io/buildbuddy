//go:build !linux

package main

import "context"

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}
