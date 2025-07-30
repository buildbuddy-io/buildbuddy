package crypter

import (
	"crypto/cipher"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"golang.org/x/crypto/chacha20poly1305"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	EncryptedDataHeaderSignature = "BB"
	EncryptedDataHeaderVersion   = 1
	PlainTextChunkSize           = 1024 * 1024 // 1 MiB
	NonceSize                    = chacha20poly1305.NonceSizeX
	EncryptedChunkOverhead       = NonceSize + chacha20poly1305.Overhead
)

type Key struct {
	Key      []byte
	Metadata *sgpb.EncryptionMetadata
}

func GetCipher(compositeKey []byte) (cipher.AEAD, error) {
	e, err := chacha20poly1305.NewX(compositeKey)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func MakeChunkAuthHeader(chunkIndex uint32, d *repb.Digest, groupID string, lastChunk bool) []byte {
	chunk := fmt.Sprint(chunkIndex)
	if lastChunk {
		chunk = "last"
	}
	return []byte(strings.Join([]string{fmt.Sprint(EncryptedDataHeaderVersion), chunk, digest.String(d), groupID}, ","))
}
