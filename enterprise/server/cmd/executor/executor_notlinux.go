//go:build !linux

package main

import "context"

func runInNamespaceAsRoot() {
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}
