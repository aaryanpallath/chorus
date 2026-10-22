package edge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/aaryanpallath/chorus/internal/protocol"
)

// Claims identify a logged-in user. A user is the broadcaster of the channel with their own name
// and a moderator of the channels listed in ModOf.
type Claims struct {
	User    string   `json:"u"`
	ModOf   []string `json:"mod,omitempty"`
	Expires int64    `json:"exp"`
}

// Sign issues a token: base64url(claims) + "." + base64url(HMAC-SHA256(secret, claims)).
func Sign(secret []byte, c Claims) string {
	body, _ := json.Marshal(c)
	p := base64.RawURLEncoding.EncodeToString(body)
	return p + "." + base64.RawURLEncoding.EncodeToString(mac(secret, p))
}

func mac(secret []byte, payload string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(payload))
	return h.Sum(nil)
}

var errBadToken = errors.New("invalid or expired token")

// Verify checks the signature and expiry and returns the claims.
func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	p, sig, ok := strings.Cut(token, ".")
	if !ok {
		return Claims{}, errBadToken
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, mac(secret, p)) {
		return Claims{}, errBadToken
	}
	body, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		return Claims{}, errBadToken
	}
	var c Claims
	if json.Unmarshal(body, &c) != nil || c.User == "" || now.Unix() > c.Expires {
		return Claims{}, errBadToken
	}
	c.User = strings.ToLower(c.User)
	return c, nil
}

// RoleIn returns the user's role in a channel.
func (c Claims) RoleIn(channel string) protocol.Role {
	switch {
	case c.User == channel:
		return protocol.RoleBroadcaster
	case slices.Contains(c.ModOf, channel):
		return protocol.RoleModerator
	}
	return protocol.RoleViewer
}
