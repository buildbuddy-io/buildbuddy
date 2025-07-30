package crypter

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
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

type Encryptor struct {
	md           *sgpb.EncryptionMetadata
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

func NewEncryptor(ctx context.Context, key *Key, digest *repb.Digest, w interfaces.CommittedWriteCloser, groupID string, chunkSize int) (*Encryptor, error) {
	ciph, err := GetCipher(key.Key)
	if err != nil {
		return nil, err
	}
	return &Encryptor{
		md:       key.Metadata,
		ciph:     ciph,
		digest:   digest,
		groupID:  groupID,
		w:        w,
		nonceBuf: make([]byte, NonceSize),
		// We allocate enough space to store an encrypted chunk so that we can
		// do the encryption in place.
		buf:    make([]byte, chunkSize+EncryptedChunkOverhead),
		bufCap: chunkSize,
	}, nil
}

func (e *Encryptor) flushBlock(lastChunk bool) error {
	if _, err := rand.Read(e.nonceBuf); err != nil {
		return err
	}
	if _, err := e.w.Write(e.nonceBuf); err != nil {
		return err
	}

	chunkAuth := MakeChunkAuthHeader(e.chunkCounter, e.digest, e.groupID, lastChunk)
	e.chunkCounter++
	ct := e.ciph.Seal(e.buf[:0], e.nonceBuf, e.buf[:e.bufIdx], chunkAuth)
	if _, err := e.w.Write(ct); err != nil {
		return err
	}
	e.bufIdx = 0
	metrics.EncryptionEncryptedBlockCount.Inc()
	return nil
}

func (e *Encryptor) Metadata() *sgpb.EncryptionMetadata {
	return e.md
}

func (e *Encryptor) Write(p []byte) (n int, err error) {
	if !e.wroteHeader {
		if _, err := e.w.Write([]byte(EncryptedDataHeaderSignature)); err != nil {
			return 0, err
		}
		if _, err := e.w.Write([]byte{EncryptedDataHeaderVersion}); err != nil {
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
			if err := e.flushBlock(false /*=lastChunk*/); err != nil {
				return 0, err
			}
		}
	}

	return len(p), nil
}

func (e *Encryptor) Commit() error {
	if err := e.flushBlock(true /*=lastChunk*/); err != nil {
		return err
	}
	metrics.EncryptionEncryptedBlobCount.Inc()
	return e.w.Commit()
}

func (e *Encryptor) Close() error {
	return e.w.Close()
}
