package secrets_test

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/secrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
)

func TestUpdateSecret(t *testing.T) {
	te := enterprise_testenv.New(t)
	authenticator := enterprise_testauth.Configure(t, te)

	// Get slices of users by gid with the admin user at the beginning
	groups := make(map[string][]*tables.User, 12)
	gids := make([]string, 12)
	for _, u := range enterprise_testauth.CreateRandomGroups(t, te) {
		for _, g := range u.Groups {
			gid := g.Group.GroupID
			if users, ok := groups[gid]; ok {
				if err := authutil.AuthorizeOrgAdmin(authenticator.UserProvider(u.UserID), gid); err == nil {
					groups[gid] = append([]*tables.User{u}, users...)
				} else {
					groups[gid] = append(users, u)
				}
			} else {
				gids = append(gids, gid)
				groups[gid] = []*tables.User{u}
			}
		}
	}

	// Generate the master key
	masterKey := make([]byte, 32)
	_, err := rand.Read(masterKey)
	require.NoError(t, err)

	// Write the master key
	masterKeyFile, err := os.OpenFile(
		testfs.MakeTempFile(t, testfs.MakeTempDir(t), "master-key-*"),
		os.O_WRONLY,
		0,
	)
	require.NoError(t, err)
	_, err = masterKeyFile.Write(masterKey)
	require.NoError(t, err)
	err = masterKeyFile.Close()
	require.NoError(t, err)

	// set up the KMS
	flags.Set(t, "keystore.master_key_uri", "local-insecure-kms://"+filepath.Base(masterKeyFile.Name()))
	flags.Set(t, "keystore.local_insecure_kms_directory", filepath.Dir(masterKeyFile.Name()))
	err = kms.Register(te)
	require.NoError(t, err)

	// set up the secret service
	flags.Set(t, "app.enable_secret_service", true)
	err = secrets.Register(te)
	require.NoError(t, err)

	secretService := te.GetSecretService()
	require.NotNil(t, secretService)

	dbh := te.GetDBHandle()
	require.NotNil(t, dbh)

	// set up the group keys
	pubKeys := make(map[string]string, len(groups))
	encPrivKeys := make(map[string]string, len(groups))
	for gid := range groups {
		pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(context.Background(), te)
		require.NoError(t, err)

		pubKeys[gid] = pubKey
		encPrivKeys[gid] = encPrivKey

		res := dbh.NewQuery(context.Background(), "update_group_keys_for_test").Raw(`
			UPDATE "Groups" SET
				public_key = ?,
				encrypted_private_key = ?
			WHERE group_id = ?`,
			pubKey,
			encPrivKey,
			gid,
		).Exec()
		require.NoError(t, res.Error)
		require.Equal(t, int64(1), res.RowsAffected)
	}

	values := make(map[string]string)
	secretName := "TEST_SECRET"
	for gid, users := range groups {
		for i, u := range users {
			values[u.UserID] = "TEST-" + u.UserID + "-" + gid
			encValue, err := keystore.NewAnonymousSealedBox(pubKeys[gid], values[u.UserID])
			require.NoError(t, err)

			userCtx, err := authenticator.WithAuthenticatedUser(context.Background(), u.UserID)
			require.NoError(t, err)

			// shared secret updated throughout the group
			rsp, newSecret, err := secretService.UpdateSecret(
				userCtx,
				&skpb.UpdateSecretRequest{
					Secret: &skpb.Secret{
						Name:  secretName,
						Value: encValue,
					},
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, i == 0, newSecret)
			assert.NotNil(t, rsp)

			// secret specific to this user
			rsp, newSecret, err = secretService.UpdateSecret(
				userCtx,
				&skpb.UpdateSecretRequest{
					Secret: &skpb.Secret{
						Name:  secretName + strconv.Itoa(i),
						Value: encValue,
					},
				},
			)
			assert.NoError(t, err)
			assert.True(t, newSecret)
			assert.NotNil(t, rsp)
		}
	}

	for gid, users := range groups {
		var count int64
		err := dbh.NewQuery(context.Background(), "verify_update_secret_for_test").Raw(`
			SELECT COUNT(*) FROM "Secrets"
			WHERE group_id = ?
			AND name = ?`,
			gid,
			secretName,
		).Take(&count)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		var encValue string
		err = dbh.NewQuery(context.Background(), "verify_update_secret_for_test").Raw(`
			SELECT value FROM "Secrets"
			WHERE group_id = ?
			AND name = ?`,
			gid,
			secretName,
		).Take(&encValue)
		assert.NoError(t, err)

		value, err := keystore.OpenAnonymousSealedBox(context.Background(), te, pubKeys[gid], encPrivKeys[gid], encValue)
		assert.NoError(t, err)

		lastUser := users[len(users)-1]
		assert.Equal(t, values[lastUser.UserID], value)

		for i, u := range users {
			var count int64
			err := te.GetDBHandle().NewQuery(context.Background(), "verify_update_secret_for_test").Raw(`
				SELECT COUNT(*) FROM "Secrets"
				WHERE group_id = ?
				AND name = ?`,
				gid,
				secretName+strconv.Itoa(i),
			).Take(&count)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), count)

			var encValue string
			err = dbh.NewQuery(context.Background(), "verify_update_secret_for_test").Raw(`
				SELECT value FROM "Secrets"
				WHERE group_id = ?
				AND name = ?`,
				gid,
				secretName+strconv.Itoa(i),
			).Take(&encValue)
			assert.NoError(t, err)

			value, err := keystore.OpenAnonymousSealedBox(context.Background(), te, pubKeys[gid], encPrivKeys[gid], encValue)
			assert.NoError(t, err)

			assert.Equal(t, values[u.UserID], value)
		}
	}
}
