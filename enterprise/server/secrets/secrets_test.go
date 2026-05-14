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
	ctx := context.Background()

	// Get slices of users by gid with the admin user at the beginning
	groups := make(map[string][]*tables.User, 12)
	for _, u := range enterprise_testauth.CreateRandomGroups(t, te) {
		for _, g := range u.Groups {
			gid := g.Group.GroupID
			if users, ok := groups[gid]; ok {
				ui, err := authenticator.UserProvider(ctx, u.UserID)
				require.NoError(t, err)
				if err := authutil.AuthorizeOrgAdmin(ui, gid); err == nil {
					groups[gid] = append([]*tables.User{u}, users...)
				} else {
					groups[gid] = append(users, u)
				}
			} else {
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
		pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(te)
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

		value, err := keystore.OpenAnonymousSealedBox(te, pubKeys[gid], encPrivKeys[gid], encValue)
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

			value, err := keystore.OpenAnonymousSealedBox(te, pubKeys[gid], encPrivKeys[gid], encValue)
			assert.NoError(t, err)

			assert.Equal(t, values[u.UserID], value)
		}
	}
}

func TestGetSecretEnvVars_Filter(t *testing.T) {
	te := enterprise_testenv.New(t)
	authenticator := enterprise_testauth.Configure(t, te)
	ctx := context.Background()

	// Generate and write a master key for KMS.
	masterKey := make([]byte, 32)
	_, err := rand.Read(masterKey)
	require.NoError(t, err)
	masterKeyFile, err := os.OpenFile(
		testfs.MakeTempFile(t, testfs.MakeTempDir(t), "master-key-*"),
		os.O_WRONLY, 0,
	)
	require.NoError(t, err)
	_, err = masterKeyFile.Write(masterKey)
	require.NoError(t, err)
	require.NoError(t, masterKeyFile.Close())
	flags.Set(t, "keystore.master_key_uri", "local-insecure-kms://"+filepath.Base(masterKeyFile.Name()))
	flags.Set(t, "keystore.local_insecure_kms_directory", filepath.Dir(masterKeyFile.Name()))
	require.NoError(t, kms.Register(te))

	flags.Set(t, "app.enable_secret_service", true)
	require.NoError(t, secrets.Register(te))

	secretService := te.GetSecretService()
	require.NotNil(t, secretService)

	// Create a user and grab their default group.
	u := enterprise_testauth.CreateRandomUser(t, te, "example.com")
	require.NotEmpty(t, u.Groups)
	gid := u.Groups[0].Group.GroupID

	// Generate and persist group keypair.
	pubKey, encPrivKey, err := keystore.GenerateSealedBoxKeys(te)
	require.NoError(t, err)
	res := te.GetDBHandle().NewQuery(ctx, "set_group_keys").Raw(`
		UPDATE "Groups" SET public_key = ?, encrypted_private_key = ? WHERE group_id = ?`,
		pubKey, encPrivKey, gid,
	).Exec()
	require.NoError(t, res.Error)

	userCtx, err := authenticator.WithAuthenticatedUser(ctx, u.UserID)
	require.NoError(t, err)

	// Store three secrets.
	for name, val := range map[string]string{
		"SECRET_A": "value-a",
		"SECRET_B": "value-b",
		"SECRET_C": "value-c",
	} {
		encValue, err := keystore.NewAnonymousSealedBox(pubKey, val)
		require.NoError(t, err)
		_, _, err = secretService.UpdateSecret(userCtx, &skpb.UpdateSecretRequest{
			Secret: &skpb.Secret{Name: name, Value: encValue},
		})
		require.NoError(t, err)
	}

	t.Run("no filter returns all secrets", func(t *testing.T) {
		envVars, err := secretService.GetSecretEnvVars(userCtx, gid)
		require.NoError(t, err)
		assert.Len(t, envVars, 3)
	})

	t.Run("single name filter", func(t *testing.T) {
		envVars, err := secretService.GetSecretEnvVars(userCtx, gid, "SECRET_A")
		require.NoError(t, err)
		require.Len(t, envVars, 1)
		assert.Equal(t, "SECRET_A", envVars[0].GetName())
		assert.Equal(t, "value-a", envVars[0].GetValue())
	})

	t.Run("multiple name filter", func(t *testing.T) {
		envVars, err := secretService.GetSecretEnvVars(userCtx, gid, "SECRET_A", "SECRET_C")
		require.NoError(t, err)
		assert.Len(t, envVars, 2)
		names := make([]string, len(envVars))
		for i, ev := range envVars {
			names[i] = ev.GetName()
		}
		assert.ElementsMatch(t, []string{"SECRET_A", "SECRET_C"}, names)
	})

	t.Run("nonexistent name returns empty", func(t *testing.T) {
		envVars, err := secretService.GetSecretEnvVars(userCtx, gid, "DOES_NOT_EXIST")
		require.NoError(t, err)
		assert.Empty(t, envVars)
	})
}
