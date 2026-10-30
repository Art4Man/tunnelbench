package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtaci/smux"
)

func init() {
	Register(newMux("tcp-mux", "TCP", tcpT{}))
	Register(newMux("ws-mux", "WebSocket", wsT{tls: false}))
	Register(newMux("wss-mux", "WebSocket", wsT{tls: true}))
}

func smuxConfig() *smux.Config {
	c := smux.DefaultConfig()
	c.Version = 2
	c.KeepAliveInterval = 10 * time.Second
	c.KeepAliveTimeout = 30 * time.Second
	c.MaxReceiveBuffer = 64 << 20
	c.MaxStreamBuffer = 16 << 20
	return c
}

// muxT multiplexes many logical streams over one underlying connection,
// so parallel-stream tests share a single carrier connection, as real
// mux tunnels do.
type muxT struct {
	name, family string
	base         Transport

	mu       sync.Mutex
	sessions map[string]*smux.Session
}

func newMux(name, family string, base Transport) *muxT {
	return &muxT{name: name, family: family, base: base, sessions: map[string]*smux.Session{}}
}

func (m *muxT) Name() string    { return m.name }
func (m *muxT) Family() string  { return m.family }
func (m *muxT) Network() string { return m.base.Network() }
func (m *muxT) NeedsRoot() bool { return m.base.NeedsRoot() }

func (m *muxT) Listen(ctx context.Context, addr string, o Options) (Listener, error) {
	bl, err := m.base.Listen(ctx, addr, o)
	if err != nil {
		return nil, err
	}
	cl := newChanListener(bl.Addr(), bl.Close)
	go func() {
		for {
			c, err := bl.Accept()
			if err != nil {
				cl.Close()
				return
			}
			go func() {
				s, err := smux.Server(c, smuxConfig())
				if err != nil {
					c.Close()
					return
				}
				defer s.Close()
				for {
					st, err := s.AcceptStream()
					if err != nil {
						return
					}
					cl.push(st)
				}
			}()
		}
	}()
	return cl, nil
}

// Reset drops the cached carrier session for addr, so the next Dial opens a
// fresh one. Called at the start of every test stage.
func (m *muxT) Reset(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.sessions[addr]; s != nil {
		s.Close()
		delete(m.sessions, addr)
	}
}

func (m *muxT) Dial(ctx context.Context, addr string, o Options) (net.Conn, error) {
	m.mu.Lock()
	s := m.sessions[addr]
	m.mu.Unlock()
	if s == nil || s.IsClosed() {
		c, err := m.base.Dial(ctx, addr, o)
		if err != nil {
			return nil, err
		}
		s, err = smux.Client(c, smuxConfig())
		if err != nil {
			c.Close()
			return nil, err
		}
		m.mu.Lock()
		if old := m.sessions[addr]; old != nil && !old.IsClosed() {
			// another goroutine won the race; keep its session
			s.Close()
			s = old
		} else {
			m.sessions[addr] = s
		}
		m.mu.Unlock()
	}
	st, err := s.OpenStream()
	if err != nil {
		m.Reset(addr)
		return nil, err
	}
	return st, nil
}
