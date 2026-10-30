package control

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Art4Man/tunnelbench/internal/plan"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

func startRunner(t *testing.T) (*Controller, transport.Options, int, chan string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	o := transport.Options{Token: "secret-token", SNI: "www.example.com"}
	warnings := make(chan string, 8)
	c, err := Listen(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), o, func(s string) {
		select {
		case warnings <- s:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, o, port, warnings
}

func newAgent(o transport.Options, port int, pin string) *Agent {
	return &Agent{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), Pin: pin, Opts: o, Version: "test", GiveUp: 2 * time.Second}
}

func TestSecondAgentIsRefused(t *testing.T) {
	c, o, port, warnings := startRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := newAgent(o, port, c.Pin())
	go func() { _ = first.Run(ctx) }()
	if _, _, err := c.WaitAgent(ctx); err != nil {
		t.Fatal(err)
	}
	second := newAgent(o, port, c.Pin())
	if err := second.Run(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("second agent: got %v, want ErrLocked", err)
	}
	select {
	case <-warnings:
	case <-time.After(5 * time.Second):
		t.Error("runner did not warn about the second agent")
	}
	// the first agent still answers
	if _, err := c.Call(ctx, Msg{Cmd: CmdClose, Addr: "0.0.0.0:" + strconv.Itoa(port+1)}); err != nil {
		t.Fatalf("first agent lost the session: %v", err)
	}
}

func TestWrongPinNeverConnects(t *testing.T) {
	c, o, port, _ := startRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := newAgent(o, port, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("got %v, want give-up error", err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer wcancel()
	if _, _, err := c.WaitAgent(wctx); err == nil {
		t.Fatal("runner accepted an agent with the wrong pin")
	}
}

func TestWrongTokenNeverConnects(t *testing.T) {
	c, o, port, _ := startRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bad := o
	bad.Token = "not-the-token"
	if err := newAgent(bad, port, c.Pin()).Run(ctx); err == nil {
		t.Fatal("agent with the wrong token ran")
	}
	wctx, wcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer wcancel()
	if _, _, err := c.WaitAgent(wctx); err == nil {
		t.Fatal("runner accepted an agent with the wrong token")
	}
}

func TestMissingPinRefused(t *testing.T) {
	_, o, port, _ := startRunner(t)
	if err := newAgent(o, port, "").Run(context.Background()); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("got %v, want missing-fingerprint error", err)
	}
}

func TestAgentLimits(t *testing.T) {
	c, o, port, _ := startRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = newAgent(o, port, c.Pin()).Run(ctx) }()
	if _, _, err := c.WaitAgent(ctx); err != nil {
		t.Fatal(err)
	}
	in := strconv.Itoa(port + 1)
	cases := []struct {
		name string
		msg  Msg
		want string
	}{
		{"foreign target", Msg{Cmd: CmdTest, Plan: &plan.Plan{Stage: plan.Probe, Method: "tcp", Addr: "10.1.2.3:" + in}}, "only the Iran server"},
		{"port outside range", Msg{Cmd: CmdTest, Plan: &plan.Plan{Stage: plan.Probe, Method: "tcp", Addr: "127.0.0.1:" + strconv.Itoa(port+PortSpan+1)}}, "outside the allowed range"},
		{"too long", Msg{Cmd: CmdTest, Plan: &plan.Plan{Stage: plan.Soak, Method: "tcp", Addr: "127.0.0.1:" + in, DurationS: plan.MaxDurationS + 1}}, "duration"},
		{"specific bind", Msg{Cmd: CmdListen, Method: "tcp", Addr: "127.0.0.1:" + in}, "not allowed"},
		{"listen out of range", Msg{Cmd: CmdListen, Method: "tcp", Addr: "0.0.0.0:22"}, "outside the allowed range"},
	}
	for _, tc := range cases {
		_, err := c.Call(ctx, tc.msg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want error containing %q", tc.name, err, tc.want)
		}
	}
	if _, err := c.Call(ctx, Msg{Cmd: CmdListen, Method: "tcp", Addr: "0.0.0.0:" + in}); err != nil {
		t.Errorf("valid listen refused: %v", err)
	}
}

func TestAgentGivesUp(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens here
	start := time.Now()
	err := newAgent(transport.Options{Token: "x"}, port, "pin").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("got %v, want give-up error", err)
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("took %s to give up", time.Since(start))
	}
}
