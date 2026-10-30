package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func init() {
	Register(wsT{tls: false})
	Register(wsT{tls: true})
}

// wsT carries the stream in binary WebSocket messages, optionally over TLS.
type wsT struct{ tls bool }

func (w wsT) Name() string {
	if w.tls {
		return "wss"
	}
	return "ws"
}
func (wsT) Family() string  { return "WebSocket" }
func (wsT) Network() string { return "tcp" }
func (wsT) NeedsRoot() bool { return false }

func wsPath(o Options) string {
	if o.Path == "" {
		return "/ws"
	}
	return o.Path
}

func (w wsT) Listen(ctx context.Context, addr string, o Options) (Listener, error) {
	l, err := listenTCP(ctx, addr)
	if err != nil {
		return nil, err
	}
	if w.tls {
		cfg, err := serverTLS(o, "http/1.1")
		if err != nil {
			l.Close()
			return nil, err
		}
		l = tls.NewListener(l, cfg)
	}
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second}
	cl := newChanListener(l.Addr(), func() error { return srv.Close() })
	up := websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}
	mux := http.NewServeMux()
	mux.HandleFunc(wsPath(o), func(rw http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(rw, r, nil)
		if err != nil {
			return
		}
		cl.push(newWSConn(c))
	})
	mux.HandleFunc("/", func(rw http.ResponseWriter, _ *http.Request) {
		http.Error(rw, "not found", http.StatusNotFound)
	})
	srv.Handler = mux
	go func() {
		_ = srv.Serve(l)
		cl.Close()
	}()
	return cl, nil
}

func (w wsT) Dial(ctx context.Context, addr string, o Options) (net.Conn, error) {
	scheme := "ws"
	d := websocket.Dialer{
		HandshakeTimeout: 8 * time.Second,
		ReadBufferSize:   64 << 10,
		WriteBufferSize:  64 << 10,
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialTCP(ctx, addr)
		},
	}
	host := addr
	if w.tls {
		scheme = "wss"
		d.TLSClientConfig = clientTLS(o, "http/1.1")
		if o.SNI != "" {
			host = o.SNI
		}
	}
	c, _, err := d.DialContext(ctx, scheme+"://"+host+wsPath(o), nil)
	if err != nil {
		return nil, err
	}
	return newWSConn(c), nil
}

// wsConn adapts a gorilla WebSocket to net.Conn.
type wsConn struct {
	*websocket.Conn
	rmu sync.Mutex
	r   io.Reader
	wmu sync.Mutex
}

func newWSConn(c *websocket.Conn) *wsConn { return &wsConn{Conn: c} }

func (c *wsConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for {
		if c.r == nil {
			_, r, err := c.NextReader()
			if err != nil {
				var ce *websocket.CloseError
				if errors.As(err, &ce) {
					return 0, io.EOF
				}
				return 0, err
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}
