package crypter_service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/time/rate"
	"gorm.io/gorm"

	mrand "math/rand"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	keyReencryptInterval = flag.Duration("crypter.key_reencrypt_interval", 6*time.Hour, "How frequently keys will be re-encrypted (to support key rotation).")
	permittedClients     = flag.Slice("crypter.permitted_clients", []string{}, "Clients (identified by clientidentity) that are permitted to access encryption keys via RPC.")
)

const (
	// How often to check for keys needing re-encryption.
	keyReencryptCheckInterval = 15 * time.Minute
	// Timeout for querying keys to re-encrypt.
	keyReencryptListQueryTimeout = 60 * time.Second
	// Timeout for re-encrypting a single key.
	keyReencryptTimeout = 60 * time.Second
	// Rate limit for re-encrypt operations.
	keyReencryptRateLimit = 50
)

// TODO(vadim): pool buffers to reduce allocations
// TODO(vadim): figure out what error codes KMS API can return
type Crypter struct {
	env      environment.Env
	dbh      interfaces.DBHandle
	kms      interfaces.KMS
	clock    clockwork.Clock
	cache    *crypter_key_cache.KeyCache
	quitChan chan struct{}
}

func Register(env *real_environment.RealEnv) error {
	if env.GetKMS() == nil {
		return nil
	}

	crypter, err := new(env, clockwork.NewRealClock())
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

func new(env environment.Env, clock clockwork.Clock) (*Crypter, error) {
	return newWithOpts(env, clock, nil)
}

func newWithOpts(env environment.Env, clock clockwork.Clock, opts *crypter_key_cache.Opts) (*Crypter, error) {
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		return refreshKey(ctx, ck, env.GetDBHandle(), env.GetKMS())
	}
	var cache *crypter_key_cache.KeyCache
	if opts == nil {
		cache = crypter_key_cache.New(env, refreshFn, clock)
	} else {
		cache = crypter_key_cache.NewWithOpts(env, refreshFn, clock, opts)
	}
	quitChan := make(chan struct{})
	cache.StartRefresher(quitChan)
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

func derivedKey(groupID string, key *tables.EncryptionKeyVersion, kms interfaces.KMS) ([]byte, error) {
	bbmk, err := kms.FetchMasterKey()
	if err != nil {
		return nil, err
	}

	gmk, err := kms.FetchKey(key.GroupKeyURI)
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

	info := append([]byte{crypter.EncryptedDataHeaderVersion}, []byte(groupID)...)
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

func refreshKey(ctx context.Context, ck crypter_key_cache.CacheKey, dbh interfaces.DBHandle, kms interfaces.KMS) ([]byte, *sgpb.EncryptionMetadata, error) {
	var query string
	var args []interface{}
	if ck.KeyID != "" {
		query = `
			SELECT * FROM "EncryptionKeyVersions" ekv
			JOIN "EncryptionKeys" ek ON ekv.encryption_key_id = ek.encryption_key_id
			WHERE ek.group_id = ? 
			AND ekv.encryption_key_id = ? AND ekv.version = ?
		`
		args = []interface{}{ck.GroupID, ck.KeyID, ck.Version}
	} else {
		query = `
			SELECT * FROM "EncryptionKeyVersions" ekv
			JOIN "EncryptionKeys" ek ON ekv.encryption_key_id = ek.encryption_key_id
			WHERE ek.group_id = ?
		`
		args = []interface{}{ck.GroupID}
	}

	ekv := &tables.EncryptionKeyVersion{}
	if err := dbh.NewQuery(ctx, "crypter_refresh_key").Raw(query, args...).Take(ekv); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, status.NotFoundError("no key available")
		}
		return nil, nil, err
	}
	key, err := derivedKey(ck.GroupID, ekv, kms)
	if err != nil {
		return nil, nil, err
	}
	md := &sgpb.EncryptionMetadata{
		EncryptionKeyId: ekv.EncryptionKeyID,
		Version:         int64(ekv.Version),
	}
	return key, md, nil
}

func (c *Crypter) newEncryptorWithChunkSize(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser, groupID string, chunkSize int) (*crypter.Encryptor, error) {
	loadedKey, err := c.cache.EncryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return crypter.NewEncryptor(ctx, loadedKey, digest, w, groupID, chunkSize)
}

func (c *Crypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	loadedKey, err := c.cache.EncryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return loadedKey.Metadata, nil
}

func (c *Crypter) NewEncryptor(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return c.newEncryptorWithChunkSize(ctx, digest, w, u.GetGroupID(), crypter.PlainTextChunkSize)
}

func (c *Crypter) newDecryptorWithChunkSize(ctx context.Context, digest *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata, groupID string, chunkSize int) (*crypter.Decryptor, error) {
	loadedKey, err := c.cache.DecryptionKey(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewDecryptor(ctx, loadedKey, digest, r, em, groupID, chunkSize)
}

func (c *Crypter) NewDecryptor(ctx context.Context, digest *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return c.newDecryptorWithChunkSize(ctx, digest, r, em, u.GetGroupID(), crypter.PlainTextChunkSize)
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
	if err := c.dbh.NewQuery(ctx, "crypter_update_key_version").Raw(q, args...).Exec().Error; err != nil {
		return err
	}

	ekv.LastEncryptedAtUsec = now.UnixMicro()

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
		for retrier.Next() {
			rq := c.dbh.NewQuery(ctx, "crypter_get_keys_to_reencrypt").Raw(q, cutoff.UnixMicro())
			ekvs, err := db.ScanAll(rq, &encryptionKeyVersionWithGroupID{})
			if err != nil {
				lastErr = err
				continue
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
		if err := c.dbh.NewQuery(uCtx, "crypter_update_encryption_timestamp").Raw(q, args...).Exec().Error; err != nil {
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
			metrics.EncryptionKeyLastEncryptedAgeMsec.Observe(float64(time.Since(time.UnixMicro(ekv.LastEncryptedAtUsec)).Milliseconds()))
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
				// Continue for loop
			}
		}
	}()
}

func (c *Crypter) Stop() {
	close(c.quitChan)
}

func (c *Crypter) testGetLastCacheRefreshRun() time.Time {
	return c.cache.TestGetLastRefreshRun()
}

func (c *Crypter) testGetCacheActiveRefreshOps() int32 {
	return c.cache.TestGetActiveRefreshOps()
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
	err = c.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		if err := tx.NewQuery(ctx, "crypter_create_key").Create(key); err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "crypter_create_key_version").Create(keyVersion); err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "crypter_group_enable_encryption").Raw(
			`UPDATE "Groups" SET cache_encryption_enabled = true WHERE group_id = ?`, u.GetGroupID()).Exec().Error; err != nil {
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
	err = c.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		q := `
			DELETE FROM "EncryptionKeyVersions"
			WHERE encryption_key_id IN (
				SELECT encryption_key_id
				FROM "EncryptionKeys"
				WHERE group_id = ?
			)
		`
		if err := tx.NewQuery(ctx, "crypter_delete_encryption_key_versions").Raw(
			q, u.GetGroupID()).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "crypter_delete_encryption_keys").Raw(
			`DELETE FROM "EncryptionKeys" where group_id = ?`, u.GetGroupID()).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "crypter_disable_group_encryption").Raw(
			`UPDATE "Groups" SET cache_encryption_enabled = false WHERE group_id = ?`, u.GetGroupID()).Exec().Error; err != nil {
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

func (c *Crypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	identityService := c.env.GetClientIdentityService()
	if identityService == nil {
		return nil, status.InternalError("Client Identity Service is required for EncryptionService")
	}
	identity, err := identityService.IdentityFromContext(ctx)
	if err != nil {
		return nil, status.InvalidArgumentError("Client Identity is required")
	}
	permitted := false
	for _, client := range *permittedClients {
		if identity.Client == client {
			permitted = true
			break
		}
	}
	if !permitted {
		return nil, status.InvalidArgumentErrorf("Client %s may not access EncryptionService", identity.Client)
	}

	var loadedKey *crypter.DerivedKey
	if req.GetMetadata().GetVersion() == 0 {
		loadedKey, err = c.cache.EncryptionKey(ctx)
	} else {
		metadata := sgpb.EncryptionMetadata{
			EncryptionKeyId: req.GetMetadata().GetId(),
			Version:         req.GetMetadata().GetVersion(),
		}
		loadedKey, err = c.cache.DecryptionKey(ctx, &metadata)
	}
	if err != nil {
		return nil, err
	}

	return &enpb.GetEncryptionKeyResponse{
		Key: &enpb.EncryptionKey{
			Metadata: &enpb.EncryptionKeyMetadata{
				Id:      loadedKey.Metadata.GetEncryptionKeyId(),
				Version: loadedKey.Metadata.GetVersion(),
			},
			Key: loadedKey.Key,
		},
	}, nil
}
