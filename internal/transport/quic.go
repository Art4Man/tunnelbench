package transport

import (
	"context"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

func init() { Register(quicT{}) }

// quicT is QUIC (TLS 1.3 over UDP) with the HTTP/3 ALPN, one stream per conn.
type quicT struct{}

func (quicT) Name() string    { return "quic" }
func (quicT) Family() string  { return "QUIC" }
func (quicT) Network() string { return "udp" }
func (quicT) NeedsRoot() bool { return false }

func quicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           8 * time.Second,
		MaxIdleTimeout:                 30 * time.Second,
		KeepAlivePeriod:                10 * time.Second,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         16 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     32 << 20,
	}
}

func (quicT) Listen(ctx context.Context, addr string, o Options) (Listener, error) {
	cfg, err := serverTLS(o, "h3")
	if err != nil {
		return nil, err
	}
	l, err := quic.ListenAddr(addr, cfg, quicConfig())
	if err != nil {
		return nil, err
	}
	cl := newChanListener(l.Addr(), l.Close)
	go func() {
		for {
			c, err := l.Accept(context.Background())
			if err != nil {
				cl.Close()
				return
			}
			go func() {
				st, err := c.AcceptStream(context.Background())
				if err != nil {
					_ = c.CloseWithError(0, "")
					return
				}
				cl.push(&quicConn{Stream: st, c: c})
			}()
		}
	}()
	return cl, nil
}

func (quicT) Dial(ctx context.Context, addr string, o Options) (net.Conn, error) {
	dctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	c, err := quic.DialAddr(dctx, addr, clientTLS(o, "h3"), quicConfig())
	if err != nil {
		return nil, err
	}
	st, err := c.OpenStreamSync(dctx)
	if err != nil {
		_ = c.CloseWithError(0, "")
		return nil, err
	}
	return &quicConn{Stream: st, c: c}, nil
}

// quicConn adapts a QUIC stream plus its connection to net.Conn.
type quicConn struct {
	quic.Stream
	c quic.Connection
}

func (q *quicConn) LocalAddr() net.Addr  { return q.c.LocalAddr() }
func (q *quicConn) RemoteAddr() net.Addr { return q.c.RemoteAddr() }

func (q *quicConn) Close() error {
	q.Stream.CancelRead(0)
	err := q.Stream.Close()
	// let queued data drain before tearing down the connection
	go func() {
		time.Sleep(2 * time.Second)
		_ = q.c.CloseWithError(0, "")
	}()
	return err
}
