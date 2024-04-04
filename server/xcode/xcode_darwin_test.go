//go:build darwin && !ios

package xcode

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXcodeLocator(t *testing.T) {
	xl := NewXcodeLocator()
	for k, v := range xl.versions {
		t.Run(k, func(t *testing.T) {
			assert.False(
				t,
				strings.HasPrefix(v.developerDirPath, "//"),
				fmt.Sprintf("developerDirPath of Xcode version %q should not has '//' prefix: %q", k, v.developerDirPath),
			)
			assert.DirExists(t, v.developerDirPath)
		})
	}
}
