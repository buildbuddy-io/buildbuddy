package serveridentity

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc/metadata"
)

const (
	IdentityHeaderName          = "x-buildbuddy-server-identity"
	validatedIdentityContextKey = "validatedServerIdentity"
	DefaultAgeTolerance         = 5 * time.Minute
	workflowAgeTolerance        = 6 * time.Hour
)

var (
	enabled        = flag.Bool("app.server_identity.enabled", false, "If true, signed headers will be used to identify servers communicating over gRPC.")
	privateKey     = flag.String("app.server_identity.private_key", "", "PEM encoded private key file in PKCS#8 format used to sign identity headers.")
	privateKeyFile = flag.String("app.server_identity.private_key_file", "", "PEM encoded private key in PKCS#8 format used to sign identity headers.")
	publicKey      = flag.String("app.server_identity.public_key", "", "PEM encoded public key file in PKIX format used to verify identity headers.")
	publicKeyFile  = flag.String("app.server_identity.public_key_file", "", "PEM encoded public key in PKIX format used to verify identity headers.")
	client         = flag.String("app.server_identity.client", "", "The client identifier to place in the identity header.")
	origin         = flag.String("app.server_identity.origin", "", "The origin identifier to place in the indentity header.")
)

type Service struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey

	clock clockwork.Clock
}

func New(clock clockwork.Clock) (*Service, error) {
	privateKey := []byte(*privateKey)
	if len(privateKey) == 0 {
		bs, err := os.ReadFile(*privateKeyFile)
		if err != nil {
			return nil, status.UnknownErrorf("could not read private key file %q: %s", *privateKeyFile, err)
		}
		privateKey = bs
	}
	rawPrivateKey, _ := pem.Decode(privateKey)
	if rawPrivateKey == nil {
		return nil, status.InvalidArgumentErrorf("could not decode private key")
	}
	parsedPrivateKey, err := x509.ParsePKCS8PrivateKey(rawPrivateKey.Bytes)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse private key: %s", err)
	}
	rsaPrivateKey, ok := parsedPrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, status.InvalidArgumentErrorf("expected an RSA key, but got %T", parsedPrivateKey)
	}

	publicKey := []byte(*publicKey)
	if len(publicKey) == 0 {
		bs, err := os.ReadFile(*publicKeyFile)
		if err != nil {
			return nil, status.UnknownErrorf("could not read public key file %q: %s", *publicKeyFile, err)
		}
		publicKey = bs
	}
	rawPublicKey, _ := pem.Decode(publicKey)
	if rawPublicKey == nil {
		return nil, status.InvalidArgumentErrorf("could not decode public key")
	}
	parsedPublicKey, err := x509.ParsePKIXPublicKey(rawPublicKey.Bytes)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse public key: %s", err)
	}
	rsaPublicKey, ok := parsedPublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, status.InvalidArgumentErrorf("expected an RSA key, but got %T", parsedPrivateKey)
	}

	return &Service{
		privateKey: rsaPrivateKey,
		publicKey:  rsaPublicKey,
		clock:      clock,
	}, nil
}

func Register(env environment.Env) error {
	if !*enabled {
		return nil
	}
	s, err := New(clockwork.NewRealClock())
	if err != nil {
		return err
	}
	env.SetServerIdentityService(s)
	return nil
}

func digest(attrs []string) []byte {
	d := sha256.Sum256([]byte(strings.Join(attrs, ",")))
	return d[:]
}

func (s *Service) AddCustomIdentityToContext(ctx context.Context, si *interfaces.ServerIdentity) (context.Context, error) {
	ts := s.clock.Now()
	attrs := []string{
		"origin=" + si.Origin,
		"client=" + si.Client,
		"timestamp=" + strconv.FormatInt(ts.UnixMicro(), 10),
	}
	d := digest(attrs)
	signature, err := rsa.SignPSS(rand.Reader, s.privateKey, crypto.SHA256, d, &rsa.PSSOptions{})
	if err != nil {
		return ctx, status.UnknownErrorf("could not sign identity data: %s", err)
	}

	attrs = append(attrs, fmt.Sprintf("signature=%x", signature))
	return metadata.AppendToOutgoingContext(ctx, IdentityHeaderName, strings.Join(attrs, "; ")), nil
}

func (s *Service) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	return s.AddCustomIdentityToContext(ctx, &interfaces.ServerIdentity{
		Origin: *origin,
		Client: *client,
	})
}

func (s *Service) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	vals := metadata.ValueFromIncomingContext(ctx, IdentityHeaderName)
	if len(vals) == 0 {
		return ctx, nil
	}
	if len(vals) > 1 {
		return ctx, status.NotFoundError("multiple identity headers present")
	}
	headerValue := vals[0]
	identity := &interfaces.ServerIdentity{}
	var attrs []string
	kvs := make(map[string]string)
	for _, attr := range strings.Split(headerValue, ";") {
		attr := strings.TrimSpace(attr)
		attrParts := strings.SplitN(attr, "=", 2)
		if len(attrParts) != 2 {
			return ctx, status.InvalidArgumentErrorf("header %q is invalid", headerValue)
		}
		k, v := attrParts[0], attrParts[1]
		if k == "signature" {
			sig, err := hex.DecodeString(v)
			if err != nil {
				return ctx, status.InvalidArgumentErrorf("invalid signature value: %s", err)
			}
			d := digest(attrs)
			if err := rsa.VerifyPSS(s.publicKey, crypto.SHA256, d, sig, &rsa.PSSOptions{}); err != nil {
				return ctx, status.InvalidArgumentErrorf("could not verify signature: %s", err)
			}
			identity.Client = kvs["client"]
			identity.Origin = kvs["origin"]
			rawTS := kvs["timestamp"]
			if rawTS == "" {
				return ctx, status.InvalidArgumentErrorf("timestamp attribute missing")
			}
			tsMicros, err := strconv.ParseInt(rawTS, 10, 64)
			if err != nil {
				return ctx, status.InvalidArgumentErrorf("could not parse timestamp: %s", err)
			}
			age := s.clock.Since(time.UnixMicro(tsMicros))
			ageTolerance := DefaultAgeTolerance
			if identity.Client == interfaces.ServerIdentityClientWorkflow {
				ageTolerance = workflowAgeTolerance
			}
			if age > ageTolerance {
				return nil, status.PermissionDeniedErrorf("identity header age %q is too old", age)
			}
			ctx = context.WithValue(ctx, validatedIdentityContextKey, identity)
			return ctx, nil
		}
		attrs = append(attrs, attr)
		kvs[k] = v
	}
	return ctx, status.InvalidArgumentErrorf("identity header could not be parsed")
}

func (s *Service) IdentityFromContext(ctx context.Context) (*interfaces.ServerIdentity, error) {
	v, ok := ctx.Value(validatedIdentityContextKey).(*interfaces.ServerIdentity)
	if !ok {
		return nil, status.NotFoundError("identity not presented")
	}
	return v, nil
}
