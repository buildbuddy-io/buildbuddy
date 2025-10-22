package clientidentity

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc/metadata"
)

const (
	DefaultExpiration = 5 * time.Minute

	cachedHeaderExpiration      = 1 * time.Minute
	validatedIdentityContextKey = "validatedClientIdentity"
)

var (
	signingKey = flag.String("app.client_identity.key", "", "The key used to sign and verify identity JWTs.", flag.Secret)
	client     = flag.String("app.client_identity.client", "", "The client identifier to place in the identity header.")
	origin     = flag.String("app.client_identity.origin", "", "The origin identifier to place in the identity header.")
	expiration = flag.Duration("app.client_identity.expiration", DefaultExpiration, "The expiration time for the identity header.")
	required   = flag.Bool("app.client_identity.required", false, "If set, a client identity is required.")
)

type Service struct {
	signingKey []byte

	clock clockwork.Clock

	mu               sync.RWMutex
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

// Clears the client-identity from the outgoing gRPC context.
func ClearIdentity(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, validatedIdentityContextKey, nil)
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = md.Copy()
		delete(md, authutil.ClientIdentityHeaderName)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func (s *Service) IdentityHeader(si *interfaces.ClientIdentity, expiration time.Duration) (string, error) {
	expirationTime := s.clock.Now().Add(expiration)
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims{
		StandardClaims: jwt.StandardClaims{ExpiresAt: expirationTime.Unix()},
		ClientIdentity: *si,
	})
	return t.SignedString(s.signingKey)
}

func (s *Service) getCachedHeader() (string, error) {
	s.mu.RLock()
	headerTime := s.cachedHeaderTime
	header := s.cachedHeader
	s.mu.RUnlock()
	if s.clock.Since(headerTime) < cachedHeaderExpiration {
		return header, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check again in case it was updated while we were waiting for the lock.
	if s.clock.Since(s.cachedHeaderTime) < cachedHeaderExpiration {
		return s.cachedHeader, nil
	}
	header, err := s.IdentityHeader(&interfaces.ClientIdentity{
		Origin: *origin,
		Client: *client,
	}, *expiration)
	if err != nil {
		return "", err
	}
	s.cachedHeader = header
	s.cachedHeaderTime = s.clock.Now()
	return header, nil
}

func (s *Service) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	header, err := s.getCachedHeader()
	if err != nil {
		return ctx, err
	}
	return metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, header), nil
}

func (s *Service) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	// Don't save the trace context so that this isn't a parent of the calls
	// that use the returned context.
	_, span := tracing.StartSpan(ctx)
	defer span.End()
	vals := metadata.ValueFromIncomingContext(ctx, authutil.ClientIdentityHeaderName)
	if len(vals) == 0 {
		if !*required {
			return ctx, nil
		}
		return nil, status.NotFoundError("identity not presented")
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
