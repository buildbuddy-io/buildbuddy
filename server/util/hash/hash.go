package hash

import (
	"crypto/sha256"
	"fmt"
)

func String(input string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
}

