package crypter_service

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"gorm.io/gorm"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	encryptedDataHeaderSignature = "BB"
	encryptedDataHeaderVersion   = 1
	plainTextChunkSize           = 1024 * 1024 // 1 MiB
	nonceSize                    = chacha20poly1305.NonceSizeX
	encryptedChunkOverhead       = nonceSize + chacha20poly1305.Overhead
)

// TODO(vadim): pool buffers to reduce allocations
type Crypter struct {
	env environment.Env
	dbh interfaces.DBHandle
	kms interfaces.KMS
}

func Register(env environment.Env) error {
	env.SetCrypter(New(env))
	return nil
}

func New(env environment.Env) *Crypter {
	return &Crypter{
		env: env,
		kms: env.GetKMS(),
		dbh: env.GetDBHandle(),
	}
}

type Encryptor struct {
	key          *tables.EncryptionKeyVersion
	ciph         cipher.AEAD
	digest       *repb.Digest
	groupID      string
	w            interfaces.CommittedWriteCloser
	wroteHeader  bool
	chunkCounter uint32
	nonceBuf     []byte

	// buf collects the plaintext until there's enough for a full chunk or the
	// is no more data left to encrypt.
	buf []byte
	// bufIdx is the index into the buffer where new data should be written.
	bufIdx int
	// bufCap is the maximum amount of plaintext the buffer can hold. The raw
	// buffer is larger to allow encryption to be done in place.
	bufCap int
}

func makeChunkAuthHeader(chunkIndex uint32, d *repb.Digest, groupID string) []byte {
	return []byte(strings.Join([]string{fmt.Sprint(chunkIndex), digest.String(d), groupID}, ","))
}

func (e *Encryptor) flushBlock() error {
	if e.bufIdx == 0 {
		return nil
	}

	if _, err := rand.Read(e.nonceBuf); err != nil {
		return err
	}
	if _, err := e.w.Write(e.nonceBuf); err != nil {
		return err
	}

	e.chunkCounter++
	chunkAuth := makeChunkAuthHeader(e.chunkCounter, e.digest, e.groupID)
	ct := e.ciph.Seal(e.buf[:0], e.nonceBuf, e.buf[:e.bufIdx], chunkAuth)
	if _, err := e.w.Write(ct); err != nil {
		return err
	}
	e.bufIdx = 0
	return nil
}

func (e *Encryptor) Metadata() *rfpb.EncryptionMetadata {
	return &rfpb.EncryptionMetadata{
		EncryptionKeyId: e.key.EncryptionKeyID,
		Version:         int64(e.key.Version),
	}
}

func (e *Encryptor) Write(p []byte) (n int, err error) {
	if !e.wroteHeader {
		if _, err := e.w.Write([]byte(encryptedDataHeaderSignature)); err != nil {
			return 0, err
		}
		if _, err := e.w.Write([]byte{encryptedDataHeaderVersion}); err != nil {
			return 0, err
		}
		e.wroteHeader = true
	}

	readIdx := 0
	for readIdx < len(p) {
		readLen := e.bufCap - e.bufIdx
		if readLen > len(p)-readIdx {
			readLen = len(p) - readIdx
		}
		copy(e.buf[e.bufIdx:], p[readIdx:readIdx+readLen])
		e.bufIdx += readLen
		readIdx += readLen
		if e.bufIdx == e.bufCap {
			if err := e.flushBlock(); err != nil {
				return 0, err
			}
		}
	}

	return len(p), nil
}

func (e *Encryptor) Commit() error {
	if err := e.flushBlock(); err != nil {
		return err
	}
	return e.w.Commit()
}

func (e *Encryptor) Close() error {
	return e.w.Close()
}

type Decryptor struct {
	ciph            cipher.AEAD
	digest          *repb.Digest
	groupID         string
	r               io.ReadCloser
	headerValidated bool
	chunkCounter    uint32

	// buf contains the decrypted plaintext ready to be read.
	buf []byte
	// bufIdx is the index at which the plaintext can be read.
	bufIdx int
	// bufLen is the amount of plaintext in the buf ready to be read.
	bufLen int
}

func (d *Decryptor) Read(p []byte) (n int, err error) {
	if !d.headerValidated {
		fileHeader := make([]byte, 3)
		if _, err := d.r.Read(fileHeader); err != nil {
			return 0, err
		}
		if string(fileHeader[0:2]) != encryptedDataHeaderSignature {
			return 0, status.InternalErrorf("invalid file signature %d %d", fileHeader[0], fileHeader[1])
		}
		if fileHeader[2] != encryptedDataHeaderVersion {
			return 0, status.InternalErrorf("invalid file version %d", fileHeader[2])
		}
		d.headerValidated = true
	}

	// No plaintext available, need to decrypt another chunk.
	if d.bufIdx >= d.bufLen {
		n, err := io.ReadFull(d.r, d.buf)
		// ErrUnexpectedEOF indicates that the underlying reader returned EOF
		// before the buffer could be filled, which is expected on the last
		// chunk.
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, err
		}

		if n < nonceSize {
			return 0, status.InternalError("could not read nonce for chunk")
		}

		d.chunkCounter++
		chunkAuth := makeChunkAuthHeader(d.chunkCounter, d.digest, d.groupID)
		nonce := d.buf[:nonceSize]
		ciphertext := d.buf[nonceSize:n]

		pt, err := d.ciph.Open(ciphertext[:0], nonce, ciphertext, chunkAuth)
		if err != nil {
			return 0, err
		}

		// We decrypted in place so the plaintext will start where the
		// ciphertext was, past the nonce.
		d.bufIdx = nonceSize
		d.bufLen = len(pt) + nonceSize
	}

	n = copy(p, d.buf[d.bufIdx:d.bufLen])
	d.bufIdx += n
	return n, nil
}

func (d *Decryptor) Close() error {
	return d.r.Close()
}

func (c *Crypter) getCipher(key *tables.EncryptionKeyVersion) (cipher.AEAD, error) {
	bbmk, err := c.kms.FetchMasterKey()
	if err != nil {
		return nil, err
	}

	gmk, err := c.kms.FetchKey(key.GroupKeyURI)
	if err != nil {
		return nil, err
	}

	masterKeyPortion, err := bbmk.Decrypt(key.MasterEncryptedKey, nil)
	if err != nil {
		return nil, err
	}
	groupKeyPortion, err := gmk.Decrypt(key.GroupEncryptedKey, nil)
	if err != nil {
		return nil, err
	}

	ckSrc := make([]byte, len(masterKeyPortion)+len(groupKeyPortion))
	ckSrc = append(ckSrc, masterKeyPortion...)
	ckSrc = append(ckSrc, groupKeyPortion...)

	compositeKey := make([]byte, 32)
	r := hkdf.Expand(sha256.New, ckSrc, nil)
	n, err := r.Read(compositeKey)
	if err != nil {
		return nil, err
	}
	if n != 32 {
		return nil, status.InternalError("invalid key length")
	}

	e, err := chacha20poly1305.NewX(compositeKey)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (c *Crypter) newEncryptorWithKey(digest *repb.Digest, w interfaces.CommittedWriteCloser, groupID string, key *tables.EncryptionKeyVersion, chunkSize int) (*Encryptor, error) {
	ciph, err := c.getCipher(key)
	if err != nil {
		return nil, err
	}
	return &Encryptor{
		key:      key,
		ciph:     ciph,
		digest:   digest,
		groupID:  groupID,
		w:        w,
		nonceBuf: make([]byte, nonceSize),
		// We allocate enough space to store an encrypted chunk so that we can
		// do the encryption in place.
		buf:    make([]byte, chunkSize+encryptedChunkOverhead),
		bufCap: chunkSize,
	}, nil
}

func (c *Crypter) NewEncryptor(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	ekv := &tables.EncryptionKeyVersion{}
	query := `
		SELECT * FROM EncryptionKeyVersions ekv
		JOIN EncryptionKeys ek ON ekv.encryption_key_id = ek.encryption_key_id
		WHERE ek.group_id = ?
	`
	if err := c.dbh.DB(ctx).Raw(query, u.GetGroupID()).Take(ekv).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.NotFoundError("no encryption key available")
		}
		return nil, err
	}
	return c.newEncryptorWithKey(digest, w, u.GetGroupID(), ekv, plainTextChunkSize)
}

func (c *Crypter) newDecryptorWithKey(digest *repb.Digest, r io.ReadCloser, groupID string, key *tables.EncryptionKeyVersion, chunkSize int) (*Decryptor, error) {
	ciph, err := c.getCipher(key)
	if err != nil {
		return nil, err
	}
	return &Decryptor{
		ciph:    ciph,
		digest:  digest,
		groupID: groupID,
		r:       r,
		buf:     make([]byte, chunkSize+encryptedChunkOverhead),
	}, nil
}

func (c *Crypter) NewDecryptor(ctx context.Context, digest *repb.Digest, r io.ReadCloser, em *rfpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	ekv := &tables.EncryptionKeyVersion{}
	query := `
		SELECT * FROM EncryptionKeyVersions ekv
		JOIN EncryptionKeys ek ON ekv.encryption_key_id = ek.encryption_key_id
		WHERE ek.group_id = ? 
        AND ekv.encryption_key_id = ? AND ekv.version = ?
	`
	if err := c.dbh.DB(ctx).Raw(query, u.GetGroupID(), em.GetEncryptionKeyId(), em.GetVersion()).Take(ekv).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.NotFoundError("no encryption key available")
		}
		return nil, err
	}
	return c.newDecryptorWithKey(digest, r, u.GetGroupID(), ekv, plainTextChunkSize)
}
