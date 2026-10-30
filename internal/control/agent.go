package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/plan"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// Agent is the Kharej end: it dials the runner and does what it is told.
type Agent struct {
	Addr    string // runner control address
	Opts    transport.Options
	Version string
	Logf    func(format string, args ...any)

	mu        sync.Mutex
	listeners map[string]transport.Listener
	jobs      map[uint64]*job
	cur       *peer
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

// Run connects and serves commands, reconnecting until the runner says bye
// or ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.listeners = map[string]transport.Listener{}
	a.jobs = map[uint64]*job{}
	defer a.closeAll()
	backoff := time.Second
	for ctx.Err() == nil {
		err := a.session(ctx)
		if errors.Is(err, ErrBye) {
			return nil
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
	conn := tlsClient(raw, a.Opts)
	p := newPeer(conn)
	defer p.kill()
	stop := context.AfterFunc(ctx, p.kill)
	defer stop()

	host, _ := os.Hostname()
	names := []string{}
	for _, t := range transport.All() {
		names = append(names, t.Name())
	}
	hello := Hello{Auth: helloAuth(a.Opts.Token), Version: a.Version, Hostname: host, Root: os.Geteuid() == 0, Methods: names}
	if err := p.send(Msg{ID: 0, Cmd: CmdHello, Hello: &hello}); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := r.ReadBytes('\n'); err != nil {
		return errors.New("runner rejected the hello (wrong link or token?)")
	}
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

func (a *Agent) exec(ctx context.Context, m Msg) Msg {
	switch m.Cmd {
	case CmdListen:
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
