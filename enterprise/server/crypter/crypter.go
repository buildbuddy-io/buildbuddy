package crypter

import (
	"crypto/cipher"

	"golang.org/x/crypto/chacha20poly1305"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
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
