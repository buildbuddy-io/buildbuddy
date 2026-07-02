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

type cachedHeader struct {
	header   string
	cachedAt time.Time
}

// headerCache caches a signed identity header per client identity, re-signing
// each at most once per cachedHeaderExpiration.
type headerCache struct {
	clock clockwork.Clock
	sign  func(si *interfaces.ClientIdentity) (string, error)

	mu      sync.RWMutex
	entries map[interfaces.ClientIdentity]cachedHeader
}

func newHeaderCache(clock clockwork.Clock, expiration time.Duration, sign func(*interfaces.ClientIdentity) (string, error)) (*headerCache, error) {
	// A cached header is reused for up to cachedHeaderExpiration, so if the
	// signed JWT's own lifetime doesn't exceed that window we could hand out a
	// token that has already expired. Require expiration > cachedHeaderExpiration
	// rather than silently serving stale identities.
	if expiration <= cachedHeaderExpiration {
		return nil, status.InvalidArgumentErrorf("app.client_identity.expiration (%s) must be greater than the header cache expiration (%s)", expiration, cachedHeaderExpiration)
	}
	return &headerCache{
		clock:   clock,
		sign:    sign,
		entries: make(map[interfaces.ClientIdentity]cachedHeader),
	}, nil
}

// Get returns the cached header for si, signing and caching a fresh one when the
// cached value is missing or older than cachedHeaderExpiration.
func (c *headerCache) Get(si *interfaces.ClientIdentity) (string, error) {
	key := *si
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if ok && c.clock.Since(e.cachedAt) < cachedHeaderExpiration {
		return e.header, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Check again in case it was updated while we were waiting for the lock.
	if e, ok := c.entries[key]; ok && c.clock.Since(e.cachedAt) < cachedHeaderExpiration {
		return e.header, nil
	}
	header, err := c.sign(si)
	if err != nil {
		return "", err
	}
	c.entries[key] = cachedHeader{header: header, cachedAt: c.clock.Now()}
	return header, nil
}

type Service struct {
	signingKey []byte

	clock clockwork.Clock

	headerCache *headerCache
}

func New(clock clockwork.Clock) (*Service, error) {
	if *signingKey == "" {
		return nil, status.InvalidArgumentError("ClientIdentityService requires a signing key")
	}
	s := &Service{
		signingKey: []byte(*signingKey),
		clock:      clock,
	}
	headerCache, err := newHeaderCache(clock, *expiration, func(si *interfaces.ClientIdentity) (string, error) {
		return s.NewIdentityHeader(si, *expiration)
	})
	if err != nil {
		return nil, err
	}
	s.headerCache = headerCache
	return s, nil
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

func (s *Service) NewIdentityHeader(si *interfaces.ClientIdentity, expiration time.Duration) (string, error) {
	expirationTime := s.clock.Now().Add(expiration)
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims{
		StandardClaims: jwt.StandardClaims{ExpiresAt: expirationTime.Unix()},
		ClientIdentity: *si,
	})
	return t.SignedString(s.signingKey)
}

// CachedIdentityHeader returns a signed identity header for the given identity,
// re-signing it at most once per cachedHeaderExpiration.
func (s *Service) CachedIdentityHeader(si *interfaces.ClientIdentity) (string, error) {
	return s.headerCache.Get(si)
}

func (s *Service) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get(authutil.ClientIdentityHeaderName); len(vals) > 0 {
			return ctx, nil
		}
	}
	header, err := s.CachedIdentityHeader(&interfaces.ClientIdentity{
		Origin: *origin,
		Client: *client,
	})
	if err != nil {
		return ctx, err
	}
	return metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, header), nil
}

func (s *Service) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
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
