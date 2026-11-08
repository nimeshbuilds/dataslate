package platform

import (
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Addr, AuthMode, DatabaseURL, Issuer, Audience, ClientID string
	Scopes                                                  []string
	ExecutionBindingsFile                                   string
	ExecutionBindings                                       []ExecutionBinding
}

func LoadConfig() (Config, error) {
	c := Config{Addr: os.Getenv("DATASLATE_ADDR"), AuthMode: os.Getenv("DATASLATE_AUTH_MODE"), DatabaseURL: os.Getenv("DATASLATE_DATABASE_URL"), Issuer: os.Getenv("DATASLATE_OIDC_ISSUER"), Audience: os.Getenv("DATASLATE_OIDC_AUDIENCE"), ClientID: os.Getenv("DATASLATE_OIDC_CLIENT_ID"), Scopes: strings.Fields(os.Getenv("DATASLATE_OIDC_SCOPES")), ExecutionBindingsFile: os.Getenv("DATASLATE_EXECUTION_BINDINGS_FILE")}
	if c.Addr == "" {
		c.Addr = "127.0.0.1:8080"
	}
	if c.AuthMode == "" {
		c.AuthMode = "oidc"
	}
	if len(c.Scopes) == 0 {
		c.Scopes = []string{"openid", "profile"}
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	bindings, err := LoadExecutionBindings(c.ExecutionBindingsFile)
	if err != nil {
		return c, err
	}
	issuer := c.Issuer
	if c.AuthMode == "development" {
		issuer = "development"
	}
	for _, b := range bindings {
		for _, s := range b.AllowedSubjects {
			if s.Issuer != issuer {
				return c, errors.New("execution subject issuer must match the configured authentication issuer")
			}
		}
	}
	c.ExecutionBindings = bindings
	return c, nil
}
func (c Config) Validate() error {
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return errors.New("DATASLATE_ADDR must be host:port")
	}
	switch c.AuthMode {
	case "development":
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("development mode requires a literal loopback listen address")
		}
		if c.DatabaseURL != "" {
			return errors.New("development mode uses ephemeral memory only; unset DATASLATE_DATABASE_URL")
		}
	case "oidc":
		if c.DatabaseURL == "" || c.Audience == "" || c.ClientID == "" {
			return errors.New("OIDC mode requires database URL, API audience and browser client ID")
		}
		if c.Audience == c.ClientID {
			return errors.New("API audience must differ from browser client ID to prevent accepting browser ID tokens")
		}
		u, e := url.Parse(c.Issuer)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("OIDC issuer must be an HTTPS URL without credentials, query or fragment")
		}
	default:
		return errors.New("DATASLATE_AUTH_MODE must be oidc or development")
	}
	return nil
}
