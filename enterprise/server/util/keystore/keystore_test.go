package keystore_test

import (
	"encoding/base64"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"

	"github.com/stretchr/testify/require"
)

// This is just a xor function; shift allows us to pass different values and
// simulate using different keys.
func xorBytes(text, xorKey []byte, shift int) []byte {
	rv := make([]byte, len(text))
	for i := range text {
		rv[i] = text[i] ^ xorKey[(i+shift)%len(xorKey)]
	}
	return rv
}

type xorAEAD struct {
	shift int
}

func (p *xorAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	return xorBytes(plaintext, associatedData, p.shift), nil
}
func (p *xorAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return xorBytes(ciphertext, associatedData, p.shift), nil
}

type testKMS struct {
	shift int
}

func (k *testKMS) FetchMasterKey() (interfaces.AEAD, error) {
	return &xorAEAD{k.shift}, nil
}

func TestTestKMS(t *testing.T) {
	kms := &testKMS{}
	plaintext := []byte("woo boy wouldn't want this to get out")
	associatedData := []byte("big_secret")

	masterKey, err := kms.FetchMasterKey()
	require.NoError(t, err)

	ciphertext, err := masterKey.Encrypt(plaintext, associatedData)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, ciphertext)

	decrypted, err := masterKey.Decrypt(ciphertext, associatedData)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestGenerateKey(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetKMS(&testKMS{})

	pubKey64, encPrivKey64, err := keystore.GenerateSealedBoxKeys(te)
	require.NoError(t, err)
	require.NotNil(t, pubKey64)
	require.NotNil(t, encPrivKey64)

	pubKeySlice, err := base64.StdEncoding.DecodeString(pubKey64)
	require.NoError(t, err)
	require.Equal(t, 32, len(pubKeySlice))

	encPrivKeySlice, err := base64.StdEncoding.DecodeString(encPrivKey64)
	require.NoError(t, err)
	require.Equal(t, 32, len(encPrivKeySlice))
}

func TestBoxSealAndOpen(t *testing.T) {
	te := testenv.GetTestEnv(t)
	te.SetKMS(&testKMS{})

	pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(te)
	require.NoError(t, err)

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		plainText, err := keystore.OpenAnonymousSealedBox(te, pubKey, encPrivKey, anonSealedBox)
		require.NoError(t, err)
		require.Equal(t, "sEcRet", plainText)
	}

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		// Using different pub/priv keys to open this box should fail.
		pubKey2, encPrivKey2, err := keystore.GenerateSealedBoxKeys(te)
		require.NoError(t, err)
		plainText, err := keystore.OpenAnonymousSealedBox(te, pubKey2, encPrivKey2, anonSealedBox)
		require.Error(t, err)
		require.NotEqual(t, "sEcRet", plainText)
	}

	{
		anonSealedBox, err := keystore.NewAnonymousSealedBox(pubKey, "sEcRet")
		require.NoError(t, err)
		require.NotNil(t, anonSealedBox)

		// Using a different master key to open this box should fail.
		te2 := testenv.GetTestEnv(t)
		te2.SetKMS(&testKMS{1}) // different offset to simulate different master key
		plainText, err := keystore.OpenAnonymousSealedBox(te2, pubKey, encPrivKey, anonSealedBox)
		require.Error(t, err)
		require.NotEqual(t, "sEcRet", plainText)
	}
}
