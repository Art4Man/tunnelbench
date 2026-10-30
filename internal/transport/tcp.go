package transport

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

func init() {
	Register(tcpT{})
	Register(tlsT{})
}

func tuneTCP(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(15 * time.Second)
		_ = tc.SetReadBuffer(4 << 20)
		_ = tc.SetWriteBuffer(4 << 20)
	}
}

type tcpListener struct{ net.Listener }

func (l tcpListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		tuneTCP(c)
	}
	return c, err
}

func listenTCP(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", addr)
}

func dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 8 * time.Second}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err == nil {
		tuneTCP(c)
	}
	return c, err
}

// tcpT is plain TCP: the baseline every other method is compared with.
type tcpT struct{}

func (tcpT) Name() string    { return "tcp" }
func (tcpT) Family() string  { return "TCP" }
func (tcpT) Network() string { return "tcp" }
func (tcpT) NeedsRoot() bool { return false }

func (tcpT) Listen(ctx context.Context, addr string, _ Options) (Listener, error) {
	l, err := listenTCP(ctx, addr)
	if err != nil {
		return nil, err
	}
	return tcpListener{l}, nil
}

func (tcpT) Dial(ctx context.Context, addr string, _ Options) (net.Conn, error) {
	return dialTCP(ctx, addr)
}

// tlsT is TLS over TCP with a configurable SNI.
type tlsT struct{}

func (tlsT) Name() string    { return "tls" }
func (tlsT) Family() string  { return "TLS" }
func (tlsT) Network() string { return "tcp" }
func (tlsT) NeedsRoot() bool { return false }

func (tlsT) Listen(ctx context.Context, addr string, o Options) (Listener, error) {
	cfg, err := serverTLS(o, "h2", "http/1.1")
	if err != nil {
		return nil, err
	}
	l, err := listenTCP(ctx, addr)
	if err != nil {
		return nil, err
	}
	return tcpListener{tls.NewListener(l, cfg)}, nil
}

func (tlsT) Dial(ctx context.Context, addr string, o Options) (net.Conn, error) {
	c, err := dialTCP(ctx, addr)
	if err != nil {
		return nil, err
	}
	tc := tls.Client(c, clientTLS(o, "h2", "http/1.1"))
	hctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		c.Close()
		return nil, err
	}
	return tc, nil
}
