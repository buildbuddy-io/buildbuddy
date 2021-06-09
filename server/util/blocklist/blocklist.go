package blocklist

import (
	"crypto/sha256"
	"fmt"
)

func hashGroupID(groupID string) string {
	h := sha256.New()
	h.Write([]byte(groupID))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func IsBlockedForStatsQuery(groupID string) bool {
	return false
}
