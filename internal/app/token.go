package app

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// APIAddr returns the address the HTTP API listens on when serving.
func (a *App) APIAddr() string {
	return fmt.Sprintf("%s:%d", a.Cfg.API.Addr, a.Cfg.API.Port)
}

// APIToken returns the configured API token, or else the on-disk secret in
// <data_dir>/api_token, generating and persisting it on first use. It never
// returns an empty token without an error: an empty token would match a
// request with no Authorization header. A token that cannot be persisted is an
// error too, since the user would have nothing to read and every later call
// would mint a different one.
func (a *App) APIToken() (string, error) {
	if a.Cfg.API.Token != "" {
		return a.Cfg.API.Token, nil
	}
	path := filepath.Join(a.DataDir(), "api_token")
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("read api token: %w", err)
	}
	if tok := strings.TrimSpace(string(b)); tok != "" {
		return tok, nil
	}
	secret := make([]byte, 32)
	if _, err := randRead(secret); err != nil {
		return "", fmt.Errorf("generate api token: %w", err)
	}
	tok := fmt.Sprintf("%x", secret)
	if err := os.WriteFile(path, []byte(tok), 0o600); err != nil {
		return "", fmt.Errorf("write api token: %w", err)
	}
	return tok, nil
}

// randRead is a variable so tests can simulate an entropy failure.
var randRead = crand.Read
