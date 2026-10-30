package control

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/plan"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// PortSpan is how many ports after the control port an agent will use.
const PortSpan = 1000

// Agent is the Kharej end: it dials the runner and does what it is told,
// within limits: it only dials the Iran host from the link, only listens on
// the port range after the control port, and caps test lengths.
type Agent struct {
	Addr    string // runner control address (host:port from the link)
	Pin     string // certificate fingerprint from the link
	Opts    transport.Options
	Version string
	GiveUp  time.Duration // stop after this long without contact (default 10m)
	Logf    func(format string, args ...any)

	id          string
	host        string
	basePort    int
	mu          sync.Mutex
	listeners   map[string]transport.Listener
	jobs        map[uint64]*job
	cur         *peer
	lastContact time.Time
}

type job struct {
	done    chan struct{}
	resp    Msg
	started time.Time
}

func (a *Agent) logf(f string, args ...any) {
	if a.Logf != nil {
		a.Logf(f, args...)
	}
}

// ErrBye is returned by Run when the runner ends the session.
var ErrBye = errors.New("runner finished the session")

func (a *Agent) touch() {
	a.mu.Lock()
	a.lastContact = time.Now()
	a.mu.Unlock()
}

func (a *Agent) sinceContact() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return time.Since(a.lastContact)
}

// Run connects and serves commands, reconnecting until the runner says bye,
// the session is refused, the runner stays unreachable for GiveUp, or ctx
// is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if a.Pin == "" {
		return errors.New("the link has no certificate fingerprint (fp=…); update tunnelbench on the Iran server and use the new link")
	}
	host, port, err := net.SplitHostPort(a.Addr)
	if err != nil {
		return err
	}
	a.host = host
	if a.basePort, err = strconv.Atoi(port); err != nil {
		return err
	}
	if a.GiveUp <= 0 {
		a.GiveUp = 10 * time.Minute
	}
	idb := make([]byte, 16)
	_, _ = rand.Read(idb)
	a.id = hex.EncodeToString(idb)
	a.listeners = map[string]transport.Listener{}
	a.jobs = map[uint64]*job{}
	a.touch()
	defer a.closeAll()

	backoff := time.Second
	for ctx.Err() == nil {
		err := a.session(ctx)
		switch {
		case errors.Is(err, ErrBye):
			return nil
		case errors.Is(err, ErrLocked), errors.Is(err, ErrBadPeer):
			return err
		}
		if a.sinceContact() > a.GiveUp {
			return fmt.Errorf("no contact with the Iran server for %s; giving up (last error: %v)", a.GiveUp, err)
		}
		a.logf("control channel down: %v (reconnecting in %s)", err, backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 15*time.Second)
	}
	return ctx.Err()
}

func (a *Agent) session(ctx context.Context) error {
	d := net.Dialer{Timeout: 10 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", a.Addr)
	if err != nil {
		return err
	}
	conn := tls.Client(raw, pinnedTLS(a.Opts, a.Pin))
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = conn.HandshakeContext(hctx)
	cancel()
	if err != nil {
		raw.Close()
		return fmt.Errorf("TLS handshake: %w", err)
	}
	ekm, err := exportKey(conn)
	if err != nil {
		conn.Close()
		return err
	}
	p := newPeer(conn)
	defer p.kill()
	stop := context.AfterFunc(ctx, p.kill)
	defer stop()

	hostname, _ := os.Hostname()
	names := []string{}
	for _, t := range transport.All() {
		names = append(names, t.Name())
	}
	hello := Hello{AgentID: a.id, Auth: proof(a.Opts.Token, "agent", a.id, ekm), Version: a.Version,
		Hostname: hostname, Root: os.Geteuid() == 0, Methods: names}
	if err := p.send(Msg{ID: 0, Cmd: CmdHello, Hello: &hello}); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	line, err := r.ReadBytes('\n')
	if err != nil {
		return errors.New("no reply to hello (wrong link or token?)")
	}
	var reply Msg
	if err := json.Unmarshal(line, &reply); err != nil {
		return ErrBadPeer
	}
	if reply.Err == lockedErr {
		return ErrLocked
	}
	if reply.Proof != proof(a.Opts.Token, "runner", a.id, ekm) {
		return ErrBadPeer
	}
	a.touch()
	a.logf("connected to runner %s", a.Addr)
	a.mu.Lock()
	a.cur = p
	a.mu.Unlock()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		a.touch()
		var m Msg
		if json.Unmarshal(line, &m) != nil {
			continue
		}
		switch m.Cmd {
		case CmdPing:
			_ = p.send(Msg{ID: m.ID, Resp: true})
		case CmdBye:
			a.logf("runner finished; exiting")
			return ErrBye
		default:
			go a.handle(ctx, m)
		}
	}
}

func (a *Agent) reply(m Msg) {
	a.mu.Lock()
	p := a.cur
	a.mu.Unlock()
	if p != nil {
		_ = p.send(m)
	}
}

// handle runs a command once per ID; a resent ID waits for the first run.
func (a *Agent) handle(ctx context.Context, m Msg) {
	a.mu.Lock()
	for id, j := range a.jobs {
		if time.Since(j.started) > 2*time.Hour {
			delete(a.jobs, id)
		}
	}
	j, seen := a.jobs[m.ID]
	if !seen {
		j = &job{done: make(chan struct{}), started: time.Now()}
		a.jobs[m.ID] = j
	}
	a.mu.Unlock()
	if !seen {
		j.resp = a.exec(ctx, m)
		j.resp.ID, j.resp.Resp = m.ID, true
		close(j.done)
	}
	<-j.done
	a.reply(j.resp)
}

// checkPort accepts only ports in the span right after the control port.
func (a *Agent) checkPort(p string) error {
	n, err := strconv.Atoi(p)
	if err != nil || n <= a.basePort || n > a.basePort+PortSpan {
		return fmt.Errorf("port %s outside the allowed range %d-%d", p, a.basePort+1, a.basePort+PortSpan)
	}
	return nil
}

// checkListen allows only wildcard binds on the test port range.
func (a *Agent) checkListen(addr string) error {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if h != "" && h != "0.0.0.0" && h != "::" {
		return fmt.Errorf("listen address %s not allowed", addr)
	}
	return a.checkPort(p)
}

// checkTarget allows dialing only the Iran host from the link.
func (a *Agent) checkTarget(addr string) error {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if h != a.host {
		return fmt.Errorf("refusing to test against %s: only the Iran server %s is allowed", h, a.host)
	}
	return a.checkPort(p)
}

func (a *Agent) exec(ctx context.Context, m Msg) Msg {
	switch m.Cmd {
	case CmdListen:
		if err := a.checkListen(m.Addr); err != nil {
			return Msg{Err: err.Error()}
		}
		tr, ok := transport.Get(m.Method)
		if !ok {
			return Msg{Err: "method not available on agent: " + m.Method}
		}
		l, err := tr.Listen(ctx, m.Addr, a.Opts)
		if err != nil {
			return Msg{Err: err.Error()}
		}
		a.mu.Lock()
		if old := a.listeners[m.Addr]; old != nil {
			old.Close()
		}
		a.listeners[m.Addr] = l
		a.mu.Unlock()
		go Serve(l, a.Opts.Token)
		return Msg{}
	case CmdClose:
		a.mu.Lock()
		if l := a.listeners[m.Addr]; l != nil {
			l.Close()
			delete(a.listeners, m.Addr)
		}
		a.mu.Unlock()
		return Msg{}
	case CmdTest:
		if m.Plan == nil {
			return Msg{Err: "test without plan"}
		}
		if err := m.Plan.Validate(); err != nil {
			return Msg{Err: err.Error()}
		}
		if err := a.checkTarget(m.Plan.Addr); err != nil {
			return Msg{Err: err.Error()}
		}
		a.logf("testing %s %s -> %s", m.Plan.Stage, m.Plan.Method, m.Plan.Addr)
		r := plan.Execute(ctx, *m.Plan, a.Opts)
		return Msg{Result: &r}
	}
	return Msg{Err: "unknown command " + m.Cmd}
}

func (a *Agent) closeAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, l := range a.listeners {
		l.Close()
		delete(a.listeners, k)
	}
}

// Serve answers measurement connections on l until it is closed.
func Serve(l transport.Listener, token string) {
	srv := &measure.Server{Token: token}
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go srv.Serve(c)
	}
}
