// Package transport defines the tunnel methods TunnelBench can test.
//
// Every method exposes the same stream interface (a net.Conn), so the
// measurement code is identical for all of them and results stay comparable.
package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"sort"
	"sync"
	"time"
)

// Options carries per-run settings shared by both ends.
type Options struct {
	Token string // shared secret from the pairing link
	SNI   string // server name used by TLS-family methods
	Path  string // HTTP path used by WebSocket methods
}

// Key derives a 32-byte key from the token for methods with their own crypto.
func (o Options) Key(purpose string) []byte {
	h := sha256.Sum256([]byte("tunnelbench:" + purpose + ":" + o.Token))
	return h[:]
}

// Listener accepts test connections for one method.
type Listener interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
}

// Transport is one tunnel method.
type Transport interface {
	Name() string    // short id used on the command line, e.g. "kcp-fec"
	Family() string  // TCP, TLS, WebSocket, UDP, QUIC
	Network() string // "tcp" or "udp": which port type the listener occupies
	NeedsRoot() bool
	Listen(ctx context.Context, addr string, o Options) (Listener, error)
	Dial(ctx context.Context, addr string, o Options) (net.Conn, error)
}

// Resetter is implemented by methods that keep state between dials (mux sessions).
type Resetter interface{ Reset(addr string) }

var (
	mu       sync.RWMutex
	registry = map[string]Transport{}
)

// Register adds a transport; called from init() in each method's file.
func Register(t Transport) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[t.Name()]; dup {
		panic("transport registered twice: " + t.Name())
	}
	registry[t.Name()] = t
}

// Get returns a transport by name.
func Get(name string) (Transport, bool) {
	mu.RLock()
	defer mu.RUnlock()
	t, ok := registry[name]
	return t, ok
}

// All returns every registered transport sorted by family then name.
func All() []Transport {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]Transport, 0, len(registry))
	for _, t := range registry {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Family() != out[j].Family() {
			return out[i].Family() < out[j].Family()
		}
		return out[i].Name() < out[j].Name()
	})
	return out
}

// chanListener turns a channel of conns into a Listener (used by WS and QUIC).
type chanListener struct {
	conns  chan net.Conn
	addr   net.Addr
	closer func() error
	once   sync.Once
	done   chan struct{}
}

func newChanListener(addr net.Addr, closer func() error) *chanListener {
	return &chanListener{conns: make(chan net.Conn, 64), addr: addr, closer: closer, done: make(chan struct{})}
}

func (l *chanListener) push(c net.Conn) {
	select {
	case l.conns <- c:
	case <-l.done:
		c.Close()
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	var err error
	l.once.Do(func() {
		close(l.done)
		if l.closer != nil {
			err = l.closer()
		}
	})
	return err
}

func (l *chanListener) Addr() net.Addr { return l.addr }

var (
	certOnce sync.Once
	cert     tls.Certificate
	certErr  error
)

// serverTLS returns a TLS config with a fresh self-signed certificate.
// Test connections are authenticated with the token, not the certificate.
func serverTLS(o Options, alpn ...string) (*tls.Config, error) {
	certOnce.Do(func() { cert, certErr = selfSigned(o.SNI) })
	if certErr != nil {
		return nil, certErr
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: alpn, MinVersion: tls.VersionTLS12}, nil
}

// CertFingerprint returns the base64url SHA-256 of this process's server
// certificate, which the pairing link carries so the agent can pin it.
func CertFingerprint(o Options) (string, error) {
	if _, err := serverTLS(o); err != nil {
		return "", err
	}
	return Fingerprint(cert.Certificate[0]), nil
}

// Fingerprint is the base64url SHA-256 of a DER certificate.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// ServerTLS and ClientTLS are used by the control channel.
func ServerTLS(o Options) (*tls.Config, error) { return serverTLS(o, "http/1.1") }
func ClientTLS(o Options) *tls.Config          { return clientTLS(o, "http/1.1") }

func clientTLS(o Options, alpn ...string) *tls.Config {
	return &tls.Config{
		ServerName:         o.SNI,
		NextProtos:         alpn,
		InsecureSkipVerify: true, //nolint:gosec // self-signed test endpoint; peer is authenticated by the token
		MinVersion:         tls.VersionTLS12,
	}
}

func selfSigned(host string) (tls.Certificate, error) {
	if host == "" {
		host = "localhost"
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
