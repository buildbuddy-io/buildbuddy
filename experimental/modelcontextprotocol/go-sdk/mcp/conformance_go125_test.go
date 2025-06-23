// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build go1.25

package mcp

import (
	"testing"
	"testing/synctest"
)

func runSyncTest(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}
