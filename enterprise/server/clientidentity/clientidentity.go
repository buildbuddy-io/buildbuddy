package clientidentity

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc/metadata"
)

const (
	IdentityHeaderName = "x-buildbuddy-client-identity"
	DefaultExpiration  = 5 * time.Minute

	cachedHeaderExpiration      = 1 * time.Minute
	validatedIdentityContextKey = "validatedClientIdentity"
)

var (
	signingKey = flag.String("app.client_identity.key", "", "The key used to sign and verify identity JWTs.", flag.Secret)
	client     = flag.String("app.client_identity.client", "", "The client identifier to place in the identity header.")
	origin     = flag.String("app.client_identity.origin", "", "The origin identifier to place in the identity header.")
)

type Service struct {
	signingKey []byte

	clock clockwork.Clock

	mu               sync.Mutex
	cachedHeader     string
	cachedHeaderTime time.Time
}

func New(clock clockwork.Clock) (*Service, error) {
	return &Service{
		signingKey: []byte(*signingKey),
		clock:      clock,
	}, nil
}

func Register(env *real_environment.RealEnv) error {
	if *signingKey == "" {
		return nil
	}
	s, err := New(clockwork.NewRealClock())
	if err != nil {
		return err
	}
	env.SetClientIdentityService(s)
	return nil
}

type claims struct {
	jwt.StandardClaims
	interfaces.ClientIdentity
}

func (s *Service) IdentityHeader(si *interfaces.ClientIdentity, expiration time.Duration) (string, error) {
	expirationTime := s.clock.Now().Add(expiration)
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims{
		StandardClaims: jwt.StandardClaims{ExpiresAt: expirationTime.Unix()},
		ClientIdentity: *si,
	})
	return t.SignedString(s.signingKey)
}

func (s *Service) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clock.Since(s.cachedHeaderTime) < cachedHeaderExpiration {
		return metadata.AppendToOutgoingContext(ctx, IdentityHeaderName, s.cachedHeader), nil
	}
	header, err := s.IdentityHeader(&interfaces.ClientIdentity{
		Origin: *origin,
		Client: *client,
	}, DefaultExpiration)
	if err != nil {
		return ctx, err
	}
	s.cachedHeader = header
	s.cachedHeaderTime = s.clock.Now()
	return metadata.AppendToOutgoingContext(ctx, IdentityHeaderName, header), nil
}

func (s *Service) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	vals := metadata.ValueFromIncomingContext(ctx, IdentityHeaderName)
	if len(vals) == 0 {
		return ctx, nil
	}
	if len(vals) > 1 {
		// When --experimental_remote_downloader is enabled in Bazel, it seems
		// to send the header twice. To workaround this, we accept the header
		// as long as it has the same value.
		if len(vals) != 2 || vals[0] != vals[1] {
			return ctx, status.PermissionDeniedError("multiple identity headers present")
		}
	}
	headerValue := vals[0]
	c := &claims{}
	if _, err := jwt.ParseWithClaims(headerValue, c, func(token *jwt.Token) (interface{}, error) {
		return s.signingKey, nil
	}); err != nil {
		return ctx, status.PermissionDeniedErrorf("invalid identity header: %s", err)
	}

	return context.WithValue(ctx, validatedIdentityContextKey, &c.ClientIdentity), nil
}

func (s *Service) IdentityFromContext(ctx context.Context) (*interfaces.ClientIdentity, error) {
	v, ok := ctx.Value(validatedIdentityContextKey).(*interfaces.ClientIdentity)
	if !ok {
		return nil, status.NotFoundError("identity not presented")
	}
	return v, nil
}
