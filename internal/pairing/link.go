// Package pairing encodes the one-line link that connects the two servers.
package pairing

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strconv"
)

// Link is everything the Kharej agent needs to reach the Iran runner.
type Link struct {
	Token string
	Host  string // Iran server address as reachable from Kharej
	Port  int    // control port
	SNI   string // SNI used by TLS-family methods
	Pin   string // SHA-256 fingerprint of the Iran server's control certificate
}

// NewToken returns a random 32-hex-character token.
func NewToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (l Link) String() string {
	u := url.URL{Scheme: "tb", User: url.User(l.Token), Host: net.JoinHostPort(l.Host, strconv.Itoa(l.Port))}
	q := url.Values{}
	if l.SNI != "" {
		q.Set("sni", l.SNI)
	}
	if l.Pin != "" {
		q.Set("fp", l.Pin)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// Addr is host:port of the control channel.
func (l Link) Addr() string { return net.JoinHostPort(l.Host, strconv.Itoa(l.Port)) }

// Parse reads a tb:// link.
func Parse(s string) (Link, error) {
	u, err := url.Parse(s)
	if err != nil {
		return Link{}, err
	}
	if u.Scheme != "tb" {
		return Link{}, fmt.Errorf("not a tunnelbench link (want tb://...)")
	}
	if u.User == nil || u.User.Username() == "" {
		return Link{}, fmt.Errorf("link has no token")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port <= 0 || port > 65535 {
		return Link{}, fmt.Errorf("link has no valid port")
	}
	return Link{Token: u.User.Username(), Host: u.Hostname(), Port: port, SNI: u.Query().Get("sni"), Pin: u.Query().Get("fp")}, nil
}
