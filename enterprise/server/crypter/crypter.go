package crypter

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/chacha20poly1305"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	// This must be exposed because it is used as an input to the
	// key-derivation function in crypter_service.
	EncryptedDataHeaderVersion = 1

	encryptedDataHeaderSignature = "BB"
	nonceSize                    = chacha20poly1305.NonceSizeX
	encryptedChunkOverhead       = nonceSize + chacha20poly1305.Overhead
)

// An encryption key. Note that this is the "derived" key as described in
// http://go/customer-managed-encryption.
type DerivedKey struct {
	Key      []byte
	Metadata *sgpb.EncryptionMetadata
}

func getCipher(compositeKey []byte) (cipher.AEAD, error) {
	e, err := chacha20poly1305.NewX(compositeKey)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func makeChunkAuthHeader(chunkIndex uint32, d *repb.Digest, groupID string, lastChunk bool) []byte {
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

func NewEncryptor(ctx context.Context, key *DerivedKey, digest *repb.Digest, w interfaces.CommittedWriteCloser, groupID string, chunkSize int) (*Encryptor, error) {
	ciph, err := getCipher(key.Key)
	if err != nil {
		return nil, err
	}
	return &Encryptor{
		md:       key.Metadata,
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

func (e *Encryptor) flushBlock(lastChunk bool) error {
	if _, err := rand.Read(e.nonceBuf); err != nil {
		return err
	}
	if _, err := e.w.Write(e.nonceBuf); err != nil {
		return err
	}

	chunkAuth := makeChunkAuthHeader(e.chunkCounter, e.digest, e.groupID, lastChunk)
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
		if _, err := e.w.Write([]byte(encryptedDataHeaderSignature)); err != nil {
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

type Decryptor struct {
	ciph               cipher.AEAD
	digest             *repb.Digest
	groupID            string
	r                  io.ReadCloser
	headerValidated    bool
	lastChunkValidated bool
	chunkCounter       uint32

	// buf contains the decrypted plaintext ready to be read.
	buf []byte
	// bufIdx is the index at which the plaintext can be read.
	bufIdx int
	// bufLen is the amount of plaintext in the buf ready to be read.
	bufLen int
}

func NewDecryptor(ctx context.Context, key *DerivedKey, digest *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata, groupID string, chunkSize int) (*Decryptor, error) {
	ciph, err := getCipher(key.Key)
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

func (d *Decryptor) Read(p []byte) (n int, err error) {
	if !d.headerValidated {
		fileHeader := make([]byte, 3)
		if _, err := d.r.Read(fileHeader); err != nil {
			return 0, err
		}
		if string(fileHeader[0:2]) != encryptedDataHeaderSignature {
			return 0, status.InternalErrorf("invalid file signature %d %d", fileHeader[0], fileHeader[1])
		}
		if fileHeader[2] != EncryptedDataHeaderVersion {
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
		lastChunk := err == io.ErrUnexpectedEOF
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF && !d.lastChunkValidated {
				return 0, status.DataLossError("did not find last chunk, file possibly truncated")
			}
			if err == io.EOF {
				metrics.EncryptionDecryptedBlobCount.Inc()
			}
			return 0, err
		}

		if n < nonceSize {
			return 0, status.InternalError("could not read nonce for chunk")
		}

		chunkAuth := makeChunkAuthHeader(d.chunkCounter, d.digest, d.groupID, lastChunk)
		d.chunkCounter++
		nonce := d.buf[:nonceSize]
		ciphertext := d.buf[nonceSize:n]

		pt, err := d.ciph.Open(ciphertext[:0], nonce, ciphertext, chunkAuth)
		if err != nil {
			metrics.EncryptionDecryptionErrorCount.Inc()
			return 0, err
		}

		metrics.EncryptionDecryptedBlockCount.Inc()

		// We decrypted in place so the plaintext will start where the
		// ciphertext was, past the nonce.
		d.bufIdx = nonceSize
		d.bufLen = len(pt) + nonceSize

		if lastChunk {
			d.lastChunkValidated = true
		}
	}

	n = copy(p, d.buf[d.bufIdx:d.bufLen])
	d.bufIdx += n
	return n, nil
}

func (d *Decryptor) Close() error {
	return d.r.Close()
}
