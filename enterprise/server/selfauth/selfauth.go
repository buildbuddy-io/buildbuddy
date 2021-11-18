package selfauth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"golang.org/x/oauth2"
)

func Provider(env environment.Env) config.OauthProvider {
	return config.OauthProvider{
		IssuerURL:    env.GetConfigurator().GetAppBuildBuddyURL(),
		ClientID:     "buildbuddy",
		ClientSecret: "secret",
	}
}

type selfAuth struct {
	env           environment.Env
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

func NewSelfAuth(env environment.Env) (*selfAuth, error) {
	// generate the same key every time; this is not meant to secure or secret
	var n, d, p, q big.Int
	n.SetString(
		"23854054546089486752143428681593425828175873941670684122390466251269713159"+
			"245737985667367468143300414391912523520647401288672254625181245443791922"+
			"131328655375075086232375550780596731379873494676833765863924320065467041"+
			"534880370068444461206120370979494670081178693685915972335443995410513383"+
			"318660804108686793740105181807521526108041735581000243371902164278468459"+
			"474539325728897031680901033148357912833321597059542124576712419669819030"+
			"691795237787947940066997209320864327657065503764510504770118773928022493"+
			"125524472354132671270720851491074770807035708210053594371353758234068435"+
			"932384388342343398119337427425316157863",
		10)
	e := 65537
	d.SetString(
		"87729721641880906844517304959404053090090039155910172788192533691556509494"+
			"987567735560150462282065075924785625588318707487505890295671690637611837"+
			"455393835620402948480850191565790777186793848085161703864712740366198041"+
			"124131644683112874933414605752286499682110876895743271922914784225644151"+
			"872939158251320289616961988397888316617229399407504281091088510280334058"+
			"317643967686673118816085972537646534312379247742468125497615074917802014"+
			"949299725127380292964176914680925563871732194797808457524468703754794888"+
			"483740034588781750869139628617153137978094331835072898153283544685228118"+
			"36352082960085761468468220014815411153",
		10)
	p.SetString(
		"16256575006379912302498183234310380159542332347031063950687283345663341727"+
			"167833048284580216390666029606428733854174688659382530985947188264451917"+
			"746355831304765087971459399499446881574719748208873179513206383348580740"+
			"482840321155574568253036132272298664418305357637788656964771883630650449"+
			"4818495674732419433",
		10)
	q.SetString(
		"14673481060265113972935553801792545032368596150582096073867660424885783011"+
			"743401917300187991295143114468294224278752892192996971113853698909746204"+
			"626408930411249605519303377705048029415871730815841134674360693227616475"+
			"494856643625318600211241561124392753685521003861493556698483525490049776"+
			"6783308172151277711",
		10)

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
		env:           env,
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
	return o.env.GetConfigurator().GetSelfAuthIssuer()
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

	u := o.env.GetConfigurator().GetSelfAuthIssuer()
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

	pictureUrl := o.IssuerURL()
	pictureUrl.Path = "/favicon/favicon_black.svg"
	token.Set("email", "buildbuddy@example.com")
	token.Set("name", "buildbuddy")
	token.Set("given_name", "Build")
	token.Set("family_name", "Buddy")
	token.Set("picture", pictureUrl.String())

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
