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
	return hashGroupID(groupID) == "143c576ef9982fe57150273eef88726a3bb37abdd427082b3a05a9460c955181"
}
