package keystore

import (
	"crypto/rand"
	"encoding/base64"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/nacl/box"
)

// GenerateSealedBoxKeys returns a new publicKey, privateKey, and error.
// The publicKey is base64encoded. It can be used (by clients) to sign new
// secret boxes that can be verified and opened by us (using our private key).
//
// The privateKey is encrypted with the master key and base64encoded. It should
// not be exposed directly to users, just stored and passed back to this
// library when opening boxes.
//
// This method should generally only be called once per customer -- to generate
// the initial keys which are then stored and used to decrypt secrets provided
// by that customer.
func GenerateSealedBoxKeys(env environment.Env) (string, string, error) {
	kms := env.GetKMS()
	if kms == nil {
		return "", "", status.FailedPreconditionError("No KMS was configured")
	}
	masterKey, err := kms.FetchMasterKey()
	if err != nil {
		return "", "", err
	}
	pubKey, privKey, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}

	encryptedPrivKey, err := masterKey.Encrypt(privKey[:], pubKey[:])
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(pubKey[:]), base64.StdEncoding.EncodeToString(encryptedPrivKey), nil
}

// OpenAnonymousSealedBoxes opens the provided anonymous sealed boxes using the
// provided publicKey and (encrypted) private key. (See GenerateSealedBoxKey
// above to generate a publicKey and privateKey). If any error is encountered it
// is immediately returned (no partial content is ever returned).
func OpenAnonymousSealedBoxes(env environment.Env, b64PublicKey, b64EncryptedPrivateKey string, b64CipherTexts []string) ([]string, error) {
	kms := env.GetKMS()
	if kms == nil {
		return nil, status.FailedPreconditionError("No KMS was configured")
	}
	masterKey, err := kms.FetchMasterKey()
	if err != nil {
		return nil, err
	}
	pubKeySlice, err := base64.StdEncoding.DecodeString(b64PublicKey)
	if err != nil {
		return nil, err
	}
	encryptedPrivateKey, err := base64.StdEncoding.DecodeString(b64EncryptedPrivateKey)
	if err != nil {
		return nil, err
	}

	privKeySlice, err := masterKey.Decrypt(encryptedPrivateKey, pubKeySlice)
	if err != nil {
		return nil, err
	}

	var pubKey, privKey [32]byte
	copy(pubKey[:], pubKeySlice[:32])
	copy(privKey[:], privKeySlice[:32])

	decoded := make([]string, 0, len(b64CipherTexts))
	for _, b64CipherText := range b64CipherTexts {
		ct, err := base64.StdEncoding.DecodeString(b64CipherText)
		if err != nil {
			return nil, err
		}
		content, ok := box.OpenAnonymous(nil, ct, &pubKey, &privKey)
		if !ok {
			return nil, status.UnavailableError("Error opening secret box")
		}
		decoded = append(decoded, string(content))
	}
	return decoded, nil
}

// OpenAnonymousSealedBox is the singlular version of the method
// OpenAnonymousSealedBoxes above.
func OpenAnonymousSealedBox(env environment.Env, b64PublicKey, b64EncryptedPrivateKey string, b64CipherText string) (string, error) {
	decoded, err := OpenAnonymousSealedBoxes(env, b64PublicKey, b64EncryptedPrivateKey, []string{b64CipherText})
	if err != nil {
		return "", err
	}
	return decoded[0], nil
}

// NewAnonymousSealedBox creates a sealed box using the provided public key.
// The returned value is a base64 encoded secret box.
// The secret box is only decodable by someone in posession of the public and
// private key.
func NewAnonymousSealedBox(b64PublicKey, plaintext string) (string, error) {
	pubKeySlice, err := base64.StdEncoding.DecodeString(b64PublicKey)
	if err != nil {
		return "", err
	}

	var pubKey [32]byte
	copy(pubKey[:], pubKeySlice[:32])

	ciphertextBytes, err := box.SealAnonymous(nil, []byte(plaintext), &pubKey, rand.Reader)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ciphertextBytes), nil
}
