//go:build !linux

package main

import "context"

func unshare() {
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}
