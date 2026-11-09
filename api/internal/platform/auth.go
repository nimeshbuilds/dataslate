package platform

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Principal struct {
	Issuer, Subject            string
	PlatformAdmin, Development bool
	Workspaces                 map[string]string
}

func (p Principal) Can(workspace string, write bool) bool {
	if p.Development {
		return true
	}
	role := p.Workspaces[workspace]
	return role == "admin" || role == "editor" || (!write && role == "viewer")
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}
type DevelopmentAuth struct{}

func (DevelopmentAuth) Authenticate(context.Context, string) (Principal, error) {
	return Principal{Issuer: "development", Subject: "development-admin", PlatformAdmin: true, Development: true}, nil
}

type JWTAuth struct{ verifier *oidc.IDTokenVerifier }

func NewJWTAuth(ctx context.Context, c Config) (*JWTAuth, error) {
	// This client is retained by the provider's remote key set. Its scoped trust,
	// HTTPS and response bounds therefore apply to both discovery and key refresh.
	client, err := newOIDCHTTPClient(c.OIDCCAFile)
	if err != nil {
		return nil, err
	}
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), c.Issuer)
	if err != nil {
		return nil, errors.New("OIDC discovery failed; verify issuer availability and trust configuration")
	}
	var metadata struct {
		JWKSURL string `json:"jwks_uri"`
	}
	if provider.Claims(&metadata) != nil || !validOIDCEndpoint(metadata.JWKSURL) {
		return nil, errors.New("OIDC discovery requires a valid HTTPS key endpoint")
	}
	return &JWTAuth{verifier: provider.Verifier(&oidc.Config{ClientID: c.Audience, SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256}})}, nil
}
func (a *JWTAuth) Authenticate(ctx context.Context, header string) (Principal, error) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > 32*1024 {
		return Principal{}, errors.New("valid bearer access token required")
	}
	token, err := a.verifier.Verify(ctx, parts[1])
	if err != nil {
		return Principal{}, errors.New("invalid access token")
	}
	var claims struct {
		Roles      []string          `json:"dataslate_roles"`
		Workspaces map[string]string `json:"dataslate_workspaces"`
		NotBefore  int64             `json:"nbf"`
		TokenUse   string            `json:"token_use"`
	}
	if token.Subject == "" || token.Expiry.IsZero() || token.Claims(&claims) != nil || claims.NotBefore > time.Now().Unix() || (claims.TokenUse != "" && claims.TokenUse != "access") {
		return Principal{}, errors.New("invalid access token claims")
	}
	p := Principal{Issuer: token.Issuer, Subject: token.Subject, Workspaces: claims.Workspaces}
	for _, r := range claims.Roles {
		if r == "platform-admin" {
			p.PlatformAdmin = true
		}
	}
	return p, nil
}
