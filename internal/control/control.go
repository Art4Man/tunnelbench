// Package control is the small command channel between the runner (Iran)
// and the agent (Kharej). The agent dials the runner, so the Kharej server
// needs no open inbound port for control.
//
// The channel itself crosses the filter, so it is built to survive being
// cut: the agent reconnects, and every request carries an ID that the agent
// de-duplicates, so the runner can resend after a reconnect without
// starting a test twice.
package control

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Art4Man/tunnelbench/internal/plan"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// Commands.
const (
	CmdHello  = "hello"
	CmdPing   = "ping"
	CmdListen = "listen"
	CmdClose  = "close"
	CmdTest   = "test"
	CmdBye    = "bye"
)

// Msg is one JSON line in either direction.
type Msg struct {
	ID     uint64       `json:"id"`
	Cmd    string       `json:"cmd,omitempty"`
	Method string       `json:"method,omitempty"`
	Addr   string       `json:"addr,omitempty"`
	Plan   *plan.Plan   `json:"plan,omitempty"`
	Hello  *Hello       `json:"hello,omitempty"`
	Resp   bool         `json:"resp,omitempty"`
	Err    string       `json:"err,omitempty"`
	Result *plan.Result `json:"result,omitempty"`
}

// Hello introduces the agent.
type Hello struct {
	Auth     string   `json:"auth"`
	Version  string   `json:"version"`
	Hostname string   `json:"hostname"`
	Root     bool     `json:"root"`
	Methods  []string `json:"methods"`
}

func helloAuth(token string) string {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte("tunnelbench:hello"))
	return hex.EncodeToString(m.Sum(nil))
}

const idleTimeout = 45 * time.Second

type peer struct {
	conn  net.Conn
	enc   *json.Encoder
	wmu   sync.Mutex
	dead  chan struct{}
	once  sync.Once
	hello Hello
}

func newPeer(c net.Conn) *peer {
	return &peer{conn: c, enc: json.NewEncoder(c), dead: make(chan struct{})}
}

func (p *peer) send(m Msg) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	_ = p.conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
	return p.enc.Encode(m)
}

func (p *peer) kill() {
	p.once.Do(func() {
		close(p.dead)
		p.conn.Close()
	})
}

func tlsClient(raw net.Conn, o transport.Options) net.Conn {
	return tls.Client(raw, transport.ClientTLS(o))
}

// ---------------------------------------------------------------- runner side

// Controller is the runner's end of the control channel.
type Controller struct {
	token string
	ln    net.Listener

	mu      sync.Mutex
	cur     *peer
	changed chan struct{} // closed and replaced whenever cur changes
	pending map[uint64]chan Msg
	nextID  atomic.Uint64
}

// Listen starts accepting agents on addr.
func Listen(addr string, o transport.Options) (*Controller, error) {
	cfg, err := transport.ServerTLS(o)
	if err != nil {
		return nil, err
	}
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	c := &Controller{token: o.Token, ln: ln, changed: make(chan struct{}), pending: map[uint64]chan Msg{}}
	go c.acceptLoop()
	go c.pingLoop()
	return c, nil
}

// Addr is the bound control address.
func (c *Controller) Addr() net.Addr { return c.ln.Addr() }

func (c *Controller) acceptLoop() {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return
		}
		go c.handshake(conn)
	}
}

func (c *Controller) handshake(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return
	}
	var m Msg
	if json.Unmarshal(line, &m) != nil || m.Cmd != CmdHello || m.Hello == nil ||
		!hmac.Equal([]byte(m.Hello.Auth), []byte(helloAuth(c.token))) {
		conn.Close()
		return
	}
	p := newPeer(conn)
	p.hello = *m.Hello
	if p.send(Msg{ID: m.ID, Resp: true}) != nil {
		conn.Close()
		return
	}
	c.mu.Lock()
	if c.cur != nil {
		c.cur.kill()
	}
	c.cur = p
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		line, err := r.ReadBytes('\n')
		if err != nil {
			p.kill()
			return
		}
		var resp Msg
		if json.Unmarshal(line, &resp) != nil || !resp.Resp {
			continue
		}
		c.mu.Lock()
		ch := c.pending[resp.ID]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- resp:
			default:
			}
		}
	}
}

func (c *Controller) pingLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		p := c.cur
		c.mu.Unlock()
		if p != nil {
			_ = p.send(Msg{ID: 0, Cmd: CmdPing})
		}
	}
}

// current returns the live peer, waiting up to `wait` for one to (re)connect.
func (c *Controller) current(ctx context.Context, wait time.Duration) (*peer, error) {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		p, ch := c.cur, c.changed
		c.mu.Unlock()
		if p != nil {
			select {
			case <-p.dead:
			default:
				return p, nil
			}
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("agent not connected")
		}
	}
}

// WaitAgent blocks until an agent has joined and returns its hello.
func (c *Controller) WaitAgent(ctx context.Context) (Hello, net.Addr, error) {
	p, err := c.current(ctx, 24*time.Hour)
	if err != nil {
		return Hello{}, nil, err
	}
	return p.hello, p.conn.RemoteAddr(), nil
}

// Call sends a request and waits for its response, resending it if the
// control connection drops and the agent reconnects.
func (c *Controller) Call(ctx context.Context, m Msg) (Msg, error) {
	m.ID = c.nextID.Add(1)
	ch := make(chan Msg, 1)
	c.mu.Lock()
	c.pending[m.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, m.ID)
		c.mu.Unlock()
	}()
	for {
		p, err := c.current(ctx, 3*time.Minute)
		if err != nil {
			return Msg{}, fmt.Errorf("control channel: %w", err)
		}
		if err := p.send(m); err != nil {
			p.kill()
			continue
		}
		select {
		case resp := <-ch:
			if resp.Err != "" {
				return resp, errors.New(resp.Err)
			}
			return resp, nil
		case <-p.dead:
			continue // agent will reconnect; resend the same ID
		case <-ctx.Done():
			return Msg{}, ctx.Err()
		}
	}
}

// Close tells the agent to stop and shuts the channel.
func (c *Controller) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.mu.Lock()
	p := c.cur
	c.mu.Unlock()
	if p != nil {
		_ = p.send(Msg{ID: c.nextID.Add(1), Cmd: CmdBye})
		select {
		case <-p.dead:
		case <-ctx.Done():
		}
		p.kill()
	}
	c.ln.Close()
}
