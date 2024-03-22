package secrets

import (
	"context"
	"flag"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
)

var (
	enableSecretService = flag.Bool("app.enable_secret_service", false, "If set, secret service will be enabled")

	secretNameRegexp = regexp.MustCompile(`^[a-zA-Z_]+[a-zA-Z0-9_]*$`)
)

type SecretService struct {
	env environment.Env
}

func New(env environment.Env) *SecretService {
	return &SecretService{
		env: env,
	}
}

func Register(env *real_environment.RealEnv) error {
	if !*enableSecretService {
		return nil
	}
	if env.GetKMS() == nil {
		return status.FailedPreconditionError("KMS is required by secret service")
	}
	env.SetSecretService(New(env))
	return nil
}

func (s *SecretService) GetPublicKey(ctx context.Context, req *skpb.GetPublicKeyRequest) (*skpb.GetPublicKeyResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	udb := s.env.GetUserDB()
	if udb == nil {
		return nil, status.FailedPreconditionError("No UserDB configured")
	}
	pubKey, err := udb.GetOrCreatePublicKey(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}
	rsp := &skpb.GetPublicKeyResponse{
		PublicKey: &skpb.PublicKey{
			Id:    hash.String(u.GetGroupID() + pubKey),
			Value: pubKey,
		},
	}
	return rsp, nil
}

func (s *SecretService) listSecretsIncludingValues(ctx context.Context) (*skpb.ListSecretsResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	dbHandle := s.env.GetDBHandle()
	if dbHandle == nil {
		return nil, status.FailedPreconditionError("A database is required")
	}

	q := query_builder.NewQuery(`SELECT name, value FROM "Secrets"`)
	q.AddWhereClause("group_id = ?", u.GetGroupID())
	q.SetOrderBy("name", true /*ascending*/)
	queryStr, args := q.Build()
	rq := dbHandle.NewQuery(ctx, "secrets_list").Raw(queryStr, args...)
	rsp := &skpb.ListSecretsResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, k *tables.Secret) error {
		rsp.Secret = append(rsp.Secret, &skpb.Secret{
			Name:  k.Name,
			Value: k.Value,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (s *SecretService) ListSecrets(ctx context.Context, req *skpb.ListSecretsRequest) (*skpb.ListSecretsResponse, error) {
	rsp, err := s.listSecretsIncludingValues(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range rsp.Secret {
		// N.B. Omit the value; the frontend doesn't need it and
		// we don't want these transiting the network any more
		// than necessary.
		s.Value = ""
	}
	return rsp, nil
}

func (s *SecretService) UpdateSecret(ctx context.Context, req *skpb.UpdateSecretRequest) (*skpb.UpdateSecretResponse, bool, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, false, err
	}
	dbHandle := s.env.GetDBHandle()
	if dbHandle == nil {
		return nil, false, status.FailedPreconditionError("A database is required")
	}

	if req.GetSecret().GetName() == "" {
		return nil, false, status.InvalidArgumentError("A non-empty secret name is required")
	}
	if req.GetSecret().GetValue() == "" {
		return nil, false, status.InvalidArgumentError("A non-empty secret value is required")
	}
	if !secretNameRegexp.MatchString(req.GetSecret().GetName()) {
		return nil, false, status.InvalidArgumentError("Secret names may only contain: [a-zA-Z0-9_]")
	}
	udb := s.env.GetUserDB()
	if udb == nil {
		return nil, false, status.FailedPreconditionError("No UserDB configured")
	}
	grp, err := udb.GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return nil, false, err
	}

	secretPerms := perms.DefaultPermissions(u)

	// Before writing the secret to the database, verify that we can open
	// the secret box using this group's public key.
	_, err = keystore.OpenAnonymousSealedBox(s.env, grp.PublicKey, grp.EncryptedPrivateKey, req.GetSecret().GetValue())
	if err != nil {
		return nil, false, err
	}

	newSecret := false
	err = dbHandle.Transaction(ctx, func(tx interfaces.DB) error {
		// TODO(zoey): remove this SELECT and replace with INSERT
		// that does nothing on conflict and then run UPDATE if zero
		// rows are affected.
		var secret tables.Secret
		err := tx.NewQuery(ctx, "secrets_get_for_update)").Raw(`
			SELECT * 
			FROM "Secrets" 
			WHERE group_id = ? AND name = ?
			`+dbHandle.SelectForUpdateModifier(), u.GetGroupID(), req.GetSecret().GetName()).Take(&secret)
		existingSecret := true
		if err != nil {
			if db.IsRecordNotFound(err) {
				existingSecret = false
			} else {
				return err
			}
		}
		if existingSecret {
			err = tx.NewQuery(ctx, "secrets_update_secret").Raw(`
				UPDATE "Secrets"
				SET value = ?
				WHERE group_id = ? AND name = ?`,
				req.GetSecret().GetValue(), u.GetGroupID(), req.GetSecret().GetName()).Exec().Error
			if err != nil {
				return err
			}
		} else {
			err = tx.NewQuery(ctx, "secrets_insert_secret").Raw(
				`INSERT INTO "Secrets" (user_id, group_id, name, value, perms) VALUES(?, ?, ?, ?, ?)`,
				u.GetUserID(), u.GetGroupID(), req.GetSecret().GetName(), req.GetSecret().GetValue(), secretPerms.Perms).Exec().Error
			if err != nil {
				return err
			}
			newSecret = true
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return &skpb.UpdateSecretResponse{}, newSecret, nil
}

func (s *SecretService) DeleteSecret(ctx context.Context, req *skpb.DeleteSecretRequest) (*skpb.DeleteSecretResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	dbHandle := s.env.GetDBHandle()
	if dbHandle == nil {
		return nil, status.FailedPreconditionError("A database is required")
	}

	if req.GetSecret().GetName() == "" {
		return nil, status.InvalidArgumentError("A non-empty secret name is required")
	}

	err = dbHandle.NewQuery(ctx, "secrets_delete").Raw(
		`DELETE FROM "Secrets" WHERE group_id = ? AND name = ?`, u.GetGroupID(), req.GetSecret().GetName()).Exec().Error
	if err != nil {
		return nil, err
	}
	return &skpb.DeleteSecretResponse{}, nil
}

func (s *SecretService) GetSecretEnvVars(ctx context.Context, groupID string) ([]*repb.Command_EnvironmentVariable, error) {
	if err := authutil.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}

	udb := s.env.GetUserDB()
	if udb == nil {
		return nil, status.FailedPreconditionError("No UserDB configured")
	}

	grp, err := udb.GetGroupByID(ctx, groupID)
	if err != nil {
		return nil, err
	}

	rsp, err := s.listSecretsIncludingValues(ctx)
	if err != nil {
		return nil, err
	}

	// No secrets, or public key not set up? Let's exit early instead of throwing
	// an error later.
	if len(rsp.GetSecret()) == 0 || grp.PublicKey == "" {
		return []*repb.Command_EnvironmentVariable{}, nil
	}

	names := make([]string, 0, len(rsp.GetSecret()))
	encValues := make([]string, 0, len(rsp.GetSecret()))
	for _, nameAndEncValue := range rsp.GetSecret() {
		names = append(names, nameAndEncValue.GetName())
		encValues = append(encValues, nameAndEncValue.GetValue())
	}

	values, err := keystore.OpenAnonymousSealedBoxes(s.env, grp.PublicKey, grp.EncryptedPrivateKey, encValues)
	if err != nil {
		return nil, err
	}

	envVars := make([]*repb.Command_EnvironmentVariable, len(values))
	for i := 0; i < len(values); i++ {
		envVars[i] = &repb.Command_EnvironmentVariable{
			Name:  names[i],
			Value: values[i],
		}
	}
	return envVars, nil
}
