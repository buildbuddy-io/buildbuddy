package selfauth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"flag"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"golang.org/x/oauth2"

	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
)

var enableSelfAuth = flag.Bool("auth.enable_self_auth", false, "If true, enables a single user login via an oauth provider on the buildbuddy server. Recommend use only when server is behind a firewall; this option may allow anyone with access to the webpage admin rights to your buildbuddy installation. ** Enterprise only **")

const (
	nString = `
23854054546089486752143428681593425828175873941670684122390466251269713159245737
98566736746814330041439191252352064740128867225462518124544379192213132865537507
50862323755507805967313798734946768337658639243200654670415348803700684444612061
20370979494670081178693685915972335443995410513383318660804108686793740105181807
52152610804173558100024337190216427846845947453932572889703168090103314835791283
33215970595421245767124196698190306917952377879479400669972093208643276570655037
64510504770118773928022493125524472354132671270720851491074770807035708210053594
371353758234068435932384388342343398119337427425316157863`

	dString = `
87729721641880906844517304959404053090090039155910172788192533691556509494987567
73556015046228206507592478562558831870748750589029567169063761183745539383562040
29484808501915657907771867938480851617038647127403661980411241316446831128749334
14605752286499682110876895743271922914784225644151872939158251320289616961988397
88831661722939940750428109108851028033405831764396768667311881608597253764653431
23792477424681254976150749178020149492997251273802929641769146809255638717321947
97808457524468703754794888483740034588781750869139628617153137978094331835072898
15328354468522811836352082960085761468468220014815411153`

	pString = `
16256575006379912302498183234310380159542332347031063950687283345663341727167833
04828458021639066602960642873385417468865938253098594718826445191774635583130476
50879714593994994468815747197482088731795132063833485807404828403211555745682530
361322722986644183053576377886569647718836306504494818495674732419433`

	qString = `
14673481060265113972935553801792545032368596150582096073867660424885783011743401
91730018799129514311446829422427875289219299697111385369890974620462640893041124
96055193033777050480294158717308158411346743606932276164754948566436253186002112
415611243927536855210038614935566984835254900497766783308172151277711`
)

const (
	ClientID     = "buildbuddy"
	ClientSecret = "secret"
)

func IssuerURL() string {
	return build_buddy_url.String()
}

func Enabled() bool {
	return *enableSelfAuth
}

type selfAuth struct {
	rsaPrivateKey jwk.RSAPrivateKey
	rsaPublicKey  jwk.RSAPublicKey
}

type configurationJSON struct {
	Issuer                           string   `json:"issuer"`
	AuthorizationEndpoint            string   `json:"authorization_endpoint"`
	TokenEndpoint                    string   `json:"token_endpoint"`
	UserinfoEndpoint                 string   `json:"userinfo_endpoint"`
	JwksUri                          string   `json:"jwks_uri"`
	IdTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
}

type tokenJSON struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int32  `json:"expires_in"`
	IdToken      string `json:"id_token"`
}

func Register(env environment.Env) error {
	if !Enabled() {
		return nil
	}
	oauth, err := NewSelfAuth()
	if err != nil {
		return status.InternalErrorf("Error initializing self auth: %s", err)
	}
	mux := env.GetMux()
	mux.Handle(oauth.AuthorizationEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.Authorize)))
	mux.Handle(oauth.TokenEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.AccessToken)))
	mux.Handle(oauth.JwksEndpoint().Path, httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.Jwks)))
	mux.Handle("/.well-known/openid-configuration", httpfilters.SetSecurityHeaders(http.HandlerFunc(oauth.WellKnownOpenIDConfiguration)))
	return nil
}

func NewSelfAuth() (*selfAuth, error) {
	// generate the same key every time; this is not meant to secure or secret
	var n, d, p, q big.Int
	n.SetString(strings.Join(strings.Fields(nString), ""), 10)
	e := 65537
	d.SetString(strings.Join(strings.Fields(dString), ""), 10)
	p.SetString(strings.Join(strings.Fields(pString), ""), 10)
	q.SetString(strings.Join(strings.Fields(qString), ""), 10)

	privateKey := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: &n,
			E: e,
		},
		D:      &d,
		Primes: []*big.Int{&p, &q},
	}
	privateKey.Precompute()
	jwkKey, err := jwk.New(privateKey)
	if err != nil {
		return nil, status.InternalErrorf("Failed to create private key: %s\n", err)
	}
	jwkPrivateKey, ok := jwkKey.(jwk.RSAPrivateKey)
	if !ok {
		return nil, status.InternalErrorf("Expected jwk.RSAPrivateKey, got %T\n", jwkKey)
	}

	jwkKey, err = jwk.New(privateKey.PublicKey)
	if err != nil {
		return nil, status.InternalErrorf("Failed to create public key: %s\n", err)
	}
	jwkPublicKey, ok := jwkKey.(jwk.RSAPublicKey)
	if !ok {
		return nil, status.InternalErrorf("Expected jwk.RSAPublicKey, got %T\n", jwkKey)
	}
	return &selfAuth{
		rsaPrivateKey: jwkPrivateKey,
		rsaPublicKey:  jwkPublicKey,
	}, nil
}

func (o *selfAuth) WellKnownOpenIDConfiguration(w http.ResponseWriter, r *http.Request) {
	writeJSONResponse(w, r, &configurationJSON{
		Issuer:                           o.IssuerURL().String(),
		AuthorizationEndpoint:            o.AuthorizationEndpoint().String(),
		TokenEndpoint:                    o.TokenEndpoint().String(),
		JwksUri:                          o.JwksEndpoint().String(),
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
	})
}

func (o *selfAuth) IssuerURL() *url.URL {
	return build_buddy_url.WithPath("")
}

func (o *selfAuth) AuthorizationEndpoint() *url.URL {
	u := o.IssuerURL()
	u.Path = "/oauth/authorize"
	return u
}

func (o *selfAuth) TokenEndpoint() *url.URL {
	u := o.IssuerURL()
	u.Path = "/oauth/access_token"
	return u
}

func (o *selfAuth) JwksEndpoint() *url.URL {
	u := o.IssuerURL()
	u.Path = "/.well-known/jwks.json"
	return u
}

// AuthCodeURL redirects to our own server.
func (o *selfAuth) AuthCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	u := o.AuthorizationEndpoint()

	v := url.Values{}
	v.Set("state", state)

	u.RawQuery = v.Encode()
	return u.String()
}

// OAuthAuthorize handles requests to /login/oauth/authorize.
func (o *selfAuth) Authorize(w http.ResponseWriter, r *http.Request) {
	state := r.FormValue("state")

	u := o.IssuerURL()
	u.Path = "/auth/"

	v := url.Values{}
	v.Set("code", "code")
	v.Set("state", state)
	u.RawQuery = v.Encode()

	http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)

}

func writeJSONResponse(w http.ResponseWriter, r *http.Request, v interface{}) {
	rsp, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.Write(rsp)
}

// AccessToken handles requests to /login/oauth/access_token, taking the code and returning a real token.
func (o *selfAuth) AccessToken(w http.ResponseWriter, r *http.Request) {
	idKey := make([]byte, 32)
	_, err := rand.Read(idKey)
	if err != nil {
		log.Errorf("Error reading rand: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	token := jwt.New()
	token.Set(jwt.AudienceKey, "buildbuddy")
	token.Set(jwt.ExpirationKey, time.Now().Add(time.Hour).Unix())
	token.Set(jwt.JwtIDKey, base64.StdEncoding.EncodeToString(idKey))
	token.Set(jwt.IssuedAtKey, time.Now().Unix())
	token.Set(jwt.IssuerKey, o.IssuerURL().String())
	token.Set(jwt.SubjectKey, "")

	token.Set("email", "buildbuddy@example.com")
	token.Set("name", "buildbuddy")
	token.Set("given_name", "Default")

	// This value is a hash of the access token, so must match. It can be
	// computed with the following python snippet:
	//
	// base64.b64encode(hashlib.sha256("AccessToken").digest()[:16]).rstrip("=")
	//
	token.Set("at_hash", "LkjhI6Ijpj638f0mirBH2g")

	signed, err := jwt.Sign(token, jwa.RS256, o.rsaPrivateKey)
	if err != nil {
		log.Errorf("Error signing token: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSONResponse(w, r, &tokenJSON{
		AccessToken:  "AccessToken",
		RefreshToken: "RefreshToken",
		ExpiresIn:    3600,
		IdToken:      string(signed),
	})
}

// Jwks handles requests to /.well-known/jwks.json, returning the keyset for our oauth
func (o *selfAuth) Jwks(w http.ResponseWriter, r *http.Request) {
	set := jwk.NewSet()
	set.Add(o.rsaPublicKey)
	writeJSONResponse(w, r, set)
}
