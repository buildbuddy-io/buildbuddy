package crypter_service

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	mrand "math/rand"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"go.uber.org/atomic"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
	"gorm.io/gorm"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	keyTTL               = flag.Duration("crypter.key_ttl", 10*time.Minute, "The maximum amount of time a key can be cached without being re-verified before it is considered invalid.")
	keyReencryptInterval = flag.Duration("crypter.key_reencrypt_interval", 6*time.Hour, "How frequently keys will be re-encrypted (to support key rotation).")
)

const (
	encryptedDataHeaderSignature = "BB"
	encryptedDataHeaderVersion   = 1
	plainTextChunkSize           = 1024 * 1024 // 1 MiB
	nonceSize                    = chacha20poly1305.NonceSizeX
	encryptedChunkOverhead       = nonceSize + chacha20poly1305.Overhead

	// How often to check for keys that need to be refreshed.
	keyRefreshScanFrequency = 10 * time.Second
	// How long to wait after a failed refresh attempt before trying again.
	keyRefreshRetryInterval = 30 * time.Second
	keyRefreshDeadline      = 25 * time.Second
	keyErrCacheTime         = 10 * time.Second

	// How often to check for keys needing re-encryption.
	keyReencryptCheckInterval = 15 * time.Minute
	// Timeout for querying keys to re-encrypt.
	keyReencryptListQueryTimeout = 60 * time.Second
	// Timeout for re-encrypting a single key.
	keyReencryptTimeout = 60 * time.Second
	// Rate limit for re-encrypt operations.
	keyReencryptRateLimit = 50
)

// Note: there are two types of keys in the cache, one with only groupID set
// (encryption) and one with all values set (decryption).
type cacheKey struct {
	groupID string
	keyID   string
	version int
}

func (ck *cacheKey) String() string {
	if ck.keyID == "" {
		return ck.groupID
	} else {
		return fmt.Sprintf("%s/%s/%d", ck.groupID, ck.keyID, ck.version)
	}
}

type cacheEntry struct {
	err error

	mu                 sync.Mutex
	keyMetadata        *rfpb.EncryptionMetadata
	derivedKey         []byte
	lastUse            time.Time
	expiresAfter       time.Time
	lastRefreshAttempt time.Time
}

type keyCache struct {
	env   environment.Env
	dbh   interfaces.DBHandle
	kms   interfaces.KMS
	clock clockwork.Clock
	sf    singleflight.Group

	data sync.Map

	mu               sync.Mutex
	lastRefreshRun   time.Time
	activeRefreshOps atomic.Int32
}

func newKeyCache(env environment.Env, clock clockwork.Clock) (*keyCache, error) {
	kc := &keyCache{
		env:   env,
		dbh:   env.GetDBHandle(),
		kms:   env.GetKMS(),
		clock: clock,
	}
	return kc, nil
}

func (c *keyCache) checkCacheEntry(ck cacheKey, ce *cacheEntry) {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	// If we reached the expiration and the key was not refreshed, then
	// remove it from the cache.
	if c.clock.Now().After(ce.expiresAfter) {
		c.data.Delete(ck)
		return
	}

	// If the expiration is far into the future, don't do anything.
	if ce.expiresAfter.Sub(c.clock.Now()) > *keyTTL/2 {
		return
	}

	// Don't try to extend the life of the key if it hasn't been used recently.
	if c.clock.Now().Sub(ce.lastUse) > *keyTTL/2 {
		return
	}

	// Don't try to refresh the key if we already tried recently.
	if c.clock.Since(ce.lastRefreshAttempt) < keyRefreshRetryInterval {
		return
	}

	c.activeRefreshOps.Inc()
	go func() {
		defer c.activeRefreshOps.Dec()
		ctx, cancel := context.WithTimeout(c.env.GetServerContext(), keyRefreshDeadline)
		defer cancel()
		ce.mu.Lock()
		ce.lastRefreshAttempt = c.clock.Now()
		ce.mu.Unlock()
		loadedKey, err := c.refreshKey(ctx, ck, false /*=cacheErr*/)
		if err == nil {
			ce.mu.Lock()
			ce.derivedKey = loadedKey.derivedKey
			ce.keyMetadata = loadedKey.metadata
			ce.expiresAfter = c.clock.Now().Add(*keyTTL)
			ce.mu.Unlock()
		} else {
			log.Warningf("could not refresh key %q: %s", ck, err)
		}
	}()
}

func (c *keyCache) startRefresher(quitChan chan struct{}) {
	go func() {
		for {
			select {
			case <-quitChan:
				return
			case <-c.clock.After(keyRefreshScanFrequency):
				break
			}

			c.data.Range(func(key, value any) bool {
				ck := key.(cacheKey)
				ce := value.(*cacheEntry)
				c.checkCacheEntry(ck, ce)
				return true
			})

			c.mu.Lock()
			c.lastRefreshRun = c.clock.Now()
			c.mu.Unlock()
		}
	}()
}

func (c *keyCache) testGetLastRefreshRun() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRefreshRun
}

func (c *keyCache) testGetActiveRefreshOps() int32 {
	return c.activeRefreshOps.Load()
}

func (c *keyCache) derivedKey(groupID string, key *tables.EncryptionKeyVersion) ([]byte, error) {
	bbmk, err := c.kms.FetchMasterKey()
	if err != nil {
		return nil, err
	}

	gmk, err := c.kms.FetchKey(key.GroupKeyURI)
	if err != nil {
		return nil, err
	}

	masterKeyPortion, err := bbmk.Decrypt(key.MasterEncryptedKey, []byte(groupID))
	if err != nil {
		return nil, err
	}
	groupKeyPortion, err := gmk.Decrypt(key.GroupEncryptedKey, []byte(groupID))
	if err != nil {
		return nil, err
	}

	ckSrc := make([]byte, 0, len(masterKeyPortion)+len(groupKeyPortion))
	ckSrc = append(ckSrc, masterKeyPortion...)
	ckSrc = append(ckSrc, groupKeyPortion...)

	info := append([]byte{encryptedDataHeaderVersion}, []byte(groupID)...)
	derivedKey := make([]byte, 32)
	r := hkdf.Expand(sha256.New, ckSrc, info)
	n, err := r.Read(derivedKey)
	if err != nil {
		return nil, err
	}
	if n != 32 {
		return nil, status.InternalError("invalid key length")
	}
	return derivedKey, nil
}

func (c *keyCache) cacheAdd(ck cacheKey, ce *cacheEntry) {
	ce.lastUse = c.clock.Now()
	c.data.Store(ck, ce)
}

func (c *keyCache) cacheGet(ck cacheKey) (*cacheEntry, bool) {
	v, ok := c.data.Load(ck)
	if !ok {
		return nil, false
	}
	e := v.(*cacheEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastUse = c.clock.Now()
	return e, true
}

func (c *keyCache) refreshKeySingleAttempt(ctx context.Context, ck cacheKey) ([]byte, *rfpb.EncryptionMetadata, error) {
	var query string
	var args []interface{}
	if ck.keyID != "" {
		query = `
			SELECT * FROM "EncryptionKeyVersions" ekv
			JOIN "EncryptionKeys" ek ON ekv.encryption_key_id = ek.encryption_key_id
			WHERE ek.group_id = ? 
			AND ekv.encryption_key_id = ? AND ekv.version = ?
		`
		args = []interface{}{ck.groupID, ck.keyID, ck.version}
	} else {
		query = `
			SELECT * FROM "EncryptionKeyVersions" ekv
			JOIN "EncryptionKeys" ek ON ekv.encryption_key_id = ek.encryption_key_id
			WHERE ek.group_id = ?
		`
		args = []interface{}{ck.groupID}
	}

	ekv := &tables.EncryptionKeyVersion{}
	if err := c.dbh.DB(ctx).Raw(query, args...).Take(ekv).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, status.NotFoundError("no key available")
		}
		return nil, nil, err
	}
	key, err := c.derivedKey(ck.groupID, ekv)
	if err != nil {
		return nil, nil, err
	}
	md := &rfpb.EncryptionMetadata{
		EncryptionKeyId: ekv.EncryptionKeyID,
		Version:         int64(ekv.Version),
	}
	c.cacheAdd(ck, &cacheEntry{
		expiresAfter: c.clock.Now().Add(*keyTTL),
		keyMetadata:  md,
		derivedKey:   key,
	})
	return key, md, nil
}

type loadedKey struct {
	derivedKey []byte
	metadata   *rfpb.EncryptionMetadata
}

func (c *keyCache) refreshKey(ctx context.Context, ck cacheKey, cacheError bool) (*loadedKey, error) {
	v, err, _ := c.sf.Do(ck.String(), func() (interface{}, error) {
		var lastErr error
		opts := retry.DefaultOptions()
		opts.Clock = c.clock
		retrier := retry.New(ctx, opts)
		for retrier.Next() {
			key, md, err := c.refreshKeySingleAttempt(ctx, ck)
			// TODO(vadim): figure out if there are other KMS errors we can treat as immediate failures
			if err == nil || status.IsNotFoundError(err) {
				return &loadedKey{key, md}, err
			}
			lastErr = err
		}
		if cacheError {
			c.cacheAdd(ck, &cacheEntry{
				err:          lastErr,
				expiresAfter: c.clock.Now().Add(keyErrCacheTime),
			})
		}
		return nil, status.UnavailableErrorf("exhausted attempts to refresh key, last error: %s", lastErr)
	})
	if err != nil {
		return nil, err
	}
	return v.(*loadedKey), err
}

func (c *keyCache) loadKey(ctx context.Context, em *rfpb.EncryptionMetadata) (*loadedKey, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var ck cacheKey
	if em != nil {
		if em.GetEncryptionKeyId() == "" {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key ID")
		}
		if em.GetVersion() == 0 {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key version")
		}
		ck = cacheKey{groupID: u.GetGroupID(), keyID: em.GetEncryptionKeyId(), version: int(em.GetVersion())}
	} else {
		ck = cacheKey{groupID: u.GetGroupID()}
	}

	e, ok := c.cacheGet(ck)
	if ok {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.err != nil {
			return nil, e.err
		}
		return &loadedKey{e.derivedKey, e.keyMetadata}, nil
	}

	// If obtaining the key fails, cache the error.
	loadedKey, err := c.refreshKey(ctx, ck, true /*=cacheErr*/)
	if err != nil {
		log.Warningf("could not refresh key: %s", err)
		return nil, err
	}
	return loadedKey, nil
}

func (c *keyCache) encryptionKey(ctx context.Context) (*loadedKey, error) {
	return c.loadKey(ctx, nil)
}

func (c *keyCache) decryptionKey(ctx context.Context, em *rfpb.EncryptionMetadata) (*loadedKey, error) {
	if em == nil {
		return nil, status.FailedPreconditionError("encryption metadata cannot be nil")
	}
	return c.loadKey(ctx, em)
}

// TODO(vadim): pool buffers to reduce allocations
// TODO(vadim): figure out what error codes KMS API can return
type Crypter struct {
	env      environment.Env
	dbh      interfaces.DBHandle
	kms      interfaces.KMS
	clock    clockwork.Clock
	cache    *keyCache
	quitChan chan struct{}
}

func Register(env environment.Env) error {
	if env.GetKMS() == nil {
		return nil
	}

	crypter, err := New(env, clockwork.NewRealClock())
	if err != nil {
		return err
	}
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		crypter.Stop()
		return nil
	})
	env.SetCrypter(crypter)
	return nil
}

func New(env environment.Env, clock clockwork.Clock) (*Crypter, error) {
	cache, err := newKeyCache(env, clock)
	if err != nil {
		return nil, err
	}
	quitChan := make(chan struct{})
	cache.startRefresher(quitChan)
	c := &Crypter{
		env:      env,
		kms:      env.GetKMS(),
		clock:    clock,
		dbh:      env.GetDBHandle(),
		cache:    cache,
		quitChan: quitChan,
	}
	c.startKeyReencryptor(quitChan)
	return c, nil
}

type Encryptor struct {
	md           *rfpb.EncryptionMetadata
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
	return []byte(strings.Join([]string{fmt.Sprint(encryptedDataHeaderVersion), fmt.Sprint(chunkIndex), digest.String(d), groupID}, ","))
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

	chunkAuth := makeChunkAuthHeader(e.chunkCounter, e.digest, e.groupID)
	e.chunkCounter++
	ct := e.ciph.Seal(e.buf[:0], e.nonceBuf, e.buf[:e.bufIdx], chunkAuth)
	if _, err := e.w.Write(ct); err != nil {
		return err
	}
	e.bufIdx = 0
	return nil
}

func (e *Encryptor) Metadata() *rfpb.EncryptionMetadata {
	return e.md
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

		chunkAuth := makeChunkAuthHeader(d.chunkCounter, d.digest, d.groupID)
		d.chunkCounter++
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

func (c *Crypter) getCipher(compositeKey []byte) (cipher.AEAD, error) {
	e, err := chacha20poly1305.NewX(compositeKey)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (c *Crypter) newEncryptorWithChunkSize(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser, groupID string, chunkSize int) (*Encryptor, error) {
	loadedKey, err := c.cache.encryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	ciph, err := c.getCipher(loadedKey.derivedKey)
	if err != nil {
		return nil, err
	}
	return &Encryptor{
		md:       loadedKey.metadata,
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

func (c *Crypter) ActiveKey(ctx context.Context) (*rfpb.EncryptionMetadata, error) {
	loadedKey, err := c.cache.encryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return loadedKey.metadata, nil
}

func (c *Crypter) NewEncryptor(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return c.newEncryptorWithChunkSize(ctx, digest, w, u.GetGroupID(), plainTextChunkSize)
}

func (c *Crypter) newDecryptorWithChunkSize(ctx context.Context, digest *repb.Digest, r io.ReadCloser, em *rfpb.EncryptionMetadata, groupID string, chunkSize int) (*Decryptor, error) {
	loadedKey, err := c.cache.decryptionKey(ctx, em)
	if err != nil {
		return nil, err
	}
	ciph, err := c.getCipher(loadedKey.derivedKey)
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
	return c.newDecryptorWithChunkSize(ctx, digest, r, em, u.GetGroupID(), plainTextChunkSize)
}

type encryptionKeyVersionWithGroupID struct {
	GroupID string
	tables.EncryptionKeyVersion
}

func (c *Crypter) reencryptKey(ctx context.Context, ekv *encryptionKeyVersionWithGroupID) error {
	bbmk, err := c.kms.FetchMasterKey()
	if err != nil {
		return err
	}

	gmk, err := c.kms.FetchKey(ekv.GroupKeyURI)
	if err != nil {
		return err
	}

	masterKeyPortion, err := bbmk.Decrypt(ekv.MasterEncryptedKey, []byte(ekv.GroupID))
	if err != nil {
		return err
	}
	groupKeyPortion, err := gmk.Decrypt(ekv.GroupEncryptedKey, []byte(ekv.GroupID))
	if err != nil {
		return err
	}
	encMasterKeyPortion, err := bbmk.Encrypt(masterKeyPortion, []byte(ekv.GroupID))
	if err != nil {
		return err
	}
	encGroupKeyPortion, err := gmk.Encrypt(groupKeyPortion, []byte(ekv.GroupID))
	if err != nil {
		return err
	}

	q := `
		UPDATE "EncryptionKeyVersions"
		SET master_encrypted_key = ?,
			group_encrypted_key = ?,
			last_encryption_attempt_at_usec = ?,
			last_encrypted_at_usec = ?
		WHERE encryption_key_id = ? AND version = ?
	`
	now := c.clock.Now()
	args := []interface{}{encMasterKeyPortion, encGroupKeyPortion, now.UnixMicro(), now.UnixMicro(), ekv.EncryptionKeyID, ekv.Version}
	if err := c.dbh.DB(ctx).Exec(q, args...).Error; err != nil {
		return err
	}

	log.Infof("Successfully re-encrypted key %q version %d", ekv.EncryptionKeyID, ekv.Version)

	return nil
}

func (c *Crypter) keyReencryptorIteration(cutoff time.Time) error {
	lim := rate.NewLimiter(rate.Limit(keyReencryptRateLimit), 1)

	queryKeys := func() ([]*encryptionKeyVersionWithGroupID, error) {
		ctx, cancel := context.WithTimeout(c.env.GetServerContext(), keyReencryptListQueryTimeout)
		defer cancel()
		q := `
			SELECT ek.group_id, ekv.*
			FROM "EncryptionKeyVersions" ekv
			JOIN "EncryptionKeys" ek ON ek.encryption_key_id = ekv.encryption_key_id
			WHERE ekv.last_encryption_attempt_at_usec < ?
			LIMIT 1000
	`

		retrier := retry.DefaultWithContext(ctx)
		var lastErr error
		var ekvs []*encryptionKeyVersionWithGroupID
		for retrier.Next() {
			ekvs = nil
			rows, err := c.dbh.DB(ctx).Raw(q, cutoff.UnixMicro()).Rows()
			if err != nil {
				lastErr = err
				continue
			}

			for rows.Next() {
				var ekv encryptionKeyVersionWithGroupID
				if err := c.dbh.DB(ctx).ScanRows(rows, &ekv); err != nil {
					return nil, err
				}
				ekvs = append(ekvs, &ekv)
			}
			return ekvs, nil
		}

		return nil, lastErr
	}

	reencryptKey := func(ekv *encryptionKeyVersionWithGroupID) {
		qCtx, qCancel := context.WithTimeout(c.env.GetServerContext(), keyReencryptTimeout)
		defer qCancel()
		retrier := retry.DefaultWithContext(qCtx)
		for retrier.Next() {
			if err := c.reencryptKey(qCtx, ekv); err != nil {
				log.Warningf("could not reencrypt key %q: %s", ekv.EncryptionKeyID, err)
			} else {
				return
			}
		}

		// Use a separate context in case we already fully used up the time on
		// the previous one. We still want to make sure we have time to update
		// the DB.
		uCtx, uCancel := context.WithTimeout(c.env.GetServerContext(), keyReencryptTimeout/4)
		defer uCancel()
		// Update the attempt timestamp.
		q := `
				UPDATE "EncryptionKeyVersions"
				SET last_encryption_attempt_at_usec = ?
				WHERE encryption_key_id = ? AND version = ?
			`
		now := c.clock.Now()
		args := []interface{}{now.UnixMicro(), ekv.EncryptionKeyID, ekv.Version}
		if err := c.dbh.DB(uCtx).Exec(q, args...).Error; err != nil {
			log.Warningf("could not update attempt timestamp: %s", err)
		}
	}

	for {
		remainingKeys, err := queryKeys()
		if err != nil {
			return err
		}

		if len(remainingKeys) == 0 {
			break
		}

		for _, ekv := range remainingKeys {
			// We don't expect to hit this rate limit in practice. It's here
			// as a precaution.
			_ = lim.Wait(c.env.GetServerContext())
			reencryptKey(ekv)
		}
	}
	return nil
}

func (c *Crypter) startKeyReencryptor(quitChan chan struct{}) {
	// All the apps will be re-encrypting keys. We add a jitter to try to avoid
	// having apps do duplicate work.
	jitter := time.Duration(mrand.Int63n(int64(*keyReencryptInterval / 2)))
	go func() {
		for {
			cutoff := c.clock.Now().Add(-*keyReencryptInterval).Add(-jitter)

			if err := c.keyReencryptorIteration(cutoff); err != nil {
				log.Warningf("could not rencrypt keys: %s", err)
			}

			select {
			case <-quitChan:
				return
			case <-c.clock.After(keyReencryptCheckInterval):
				break
			}
		}
	}()
}

func (c *Crypter) Stop() {
	close(c.quitChan)
}

func (c *Crypter) testGetLastCacheRefreshRun() time.Time {
	return c.cache.testGetLastRefreshRun()
}

func (c *Crypter) testGetCacheActiveRefreshOps() int32 {
	return c.cache.testGetActiveRefreshOps()
}

func buildKeyURI(kmsConfig *enpb.KMSConfig) (string, error) {
	if lc := kmsConfig.GetLocalInsecureKmsConfig(); lc != nil {
		if strings.TrimSpace(lc.GetKeyId()) == "" {
			return "", status.InvalidArgumentError("Key ID is required")
		}
		return fmt.Sprintf("local-insecure-kms://%s", lc.GetKeyId()), nil
	}
	if gc := kmsConfig.GetGcpKmsConfig(); gc != nil {
		if strings.TrimSpace(gc.GetProject()) == "" {
			return "", status.InvalidArgumentError("Project is required")
		}
		if strings.TrimSpace(gc.GetLocation()) == "" {
			return "", status.InvalidArgumentError("Location is required")
		}
		if strings.TrimSpace(gc.GetKeyRing()) == "" {
			return "", status.InvalidArgumentError("Key Ring is required")
		}
		if strings.TrimSpace(gc.GetKey()) == "" {
			return "", status.InvalidArgumentError("Key is required")
		}
		return fmt.Sprintf("gcp-kms://projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", gc.GetProject(), gc.GetLocation(), gc.GetKeyRing(), gc.GetKey()), nil
	}
	if ac := kmsConfig.GetAwsKmsConfig(); ac != nil {
		if strings.TrimSpace(ac.GetKeyArn()) == "" {
			return "", status.InvalidArgumentError("Key ARN is required")
		}
		return fmt.Sprintf("aws-kms://%s", ac.GetKeyArn()), nil
	}

	return "", status.FailedPreconditionError("KMS config is empty")
}

func (c *Crypter) enableEncryption(ctx context.Context, kmsConfig *enpb.KMSConfig) error {
	groupKeyURI, err := buildKeyURI(kmsConfig)
	if err != nil {
		return err
	}

	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}

	// Get the KMS clients for the customer and our own keys. This doesn't
	// actually talk to the KMS systems yet.
	groupKeyClient, err := c.env.GetKMS().FetchKey(groupKeyURI)
	if err != nil {
		return status.UnavailableErrorf("invalid key URI: %s", err)
	}
	masterKeyClient, err := c.env.GetKMS().FetchMasterKey()
	if err != nil {
		return err
	}

	// Generate the master & group (customer) portions of the composite key.
	masterKeyPart := make([]byte, 32)
	_, err = rand.Read(masterKeyPart)
	if err != nil {
		return status.InternalErrorf("could not generate key: %s", err)
	}
	groupKeyPart := make([]byte, 32)
	_, err = rand.Read(groupKeyPart)
	if err != nil {
		return status.InternalErrorf("could not generate key: %s", err)
	}
	keyID, err := tables.PrimaryKeyForTable("EncryptionKeys")
	if err != nil {
		return status.InternalErrorf("could not generate key id: %s", err)
	}

	encMasterKeyPart, err := masterKeyClient.Encrypt(masterKeyPart, []byte(u.GetGroupID()))
	if err != nil {
		return status.InternalErrorf("could not encrypt master portion of composite key: %s", err)
	}
	// This is where we'd fail if the customer supplied an invalid key, so we
	// intentionally use a different error code here.
	encGroupKeyPart, err := groupKeyClient.Encrypt(groupKeyPart, []byte(u.GetGroupID()))
	if err != nil {
		return status.UnavailableErrorf("could not use customer key for encryption: %s", err)
	}

	// We're good to go. Now just need to update the database.

	key := &tables.EncryptionKey{
		EncryptionKeyID: keyID,
		GroupID:         u.GetGroupID(),
	}
	now := c.clock.Now()
	keyVersion := &tables.EncryptionKeyVersion{
		EncryptionKeyID:             keyID,
		Version:                     1,
		MasterEncryptedKey:          encMasterKeyPart,
		GroupKeyURI:                 groupKeyURI,
		GroupEncryptedKey:           encGroupKeyPart,
		LastEncryptionAttemptAtUsec: now.UnixMicro(),
		LastEncryptedAtUsec:         now.UnixMicro(),
	}
	err = c.env.GetDBHandle().Transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Create(key).Error; err != nil {
			return err
		}
		if err := tx.Create(keyVersion).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE "Groups" SET cache_encryption_enabled = true WHERE group_id = ?`, u.GetGroupID()).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return status.InternalErrorf("could not update key information: %s", err)
	}
	return nil
}

func (c *Crypter) disableEncryption(ctx context.Context) error {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	err = c.env.GetDBHandle().Transaction(ctx, func(tx *gorm.DB) error {
		q := `
			DELETE FROM "EncryptionKeyVersions"
			WHERE encryption_key_id IN (
				SELECT encryption_key_id
				FROM "EncryptionKeys"
				WHERE group_id = ?
			)
		`
		if err := tx.Exec(q, u.GetGroupID()).Error; err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM "EncryptionKeys" where group_id = ?`, u.GetGroupID()).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE "Groups" SET cache_encryption_enabled = false WHERE group_id = ?`, u.GetGroupID()).Error; err != nil {
			return err
		}
		return nil
	})
	return err
}

func (c *Crypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	if c.env.GetCrypter() == nil {
		return nil, status.FailedPreconditionErrorf("crypter service not enabled")
	}
	if c.env.GetKMS() == nil {
		return nil, status.FailedPreconditionErrorf("KMS service not enabled")
	}

	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	g, err := c.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}
	if g.CacheEncryptionEnabled == req.Enabled {
		return &enpb.SetEncryptionConfigResponse{}, nil
	}

	if req.Enabled {
		if err := c.enableEncryption(ctx, req.GetKmsConfig()); err != nil {
			return nil, err
		}
		return &enpb.SetEncryptionConfigResponse{}, nil
	} else {
		if err := c.disableEncryption(ctx); err != nil {
			return nil, err
		}
		return &enpb.SetEncryptionConfigResponse{}, nil
	}
}

func (c *Crypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	g, err := c.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}

	rsp := &enpb.GetEncryptionConfigResponse{
		Enabled: g.CacheEncryptionEnabled,
	}

	for _, t := range c.env.GetKMS().SupportedTypes() {
		switch t {
		case interfaces.KMSTypeLocalInsecure:
			rsp.SupportedKms = append(rsp.SupportedKms, enpb.KMS_LOCAL_INSECURE)
		case interfaces.KMSTypeGCP:
			rsp.SupportedKms = append(rsp.SupportedKms, enpb.KMS_GCP)
		case interfaces.KMSTypeAWS:
			rsp.SupportedKms = append(rsp.SupportedKms, enpb.KMS_AWS)
		default:
			log.Warningf("unknown KMS type %q", t)
		}
	}

	return rsp, err
}
