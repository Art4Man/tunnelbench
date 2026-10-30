package runner_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Art4Man/tunnelbench/internal/control"
	"github.com/Art4Man/tunnelbench/internal/pairing"
	"github.com/Art4Man/tunnelbench/internal/runner"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestRunEndToEnd pairs a runner and an agent over loopback and runs two
// methods in both modes, including a short soak.
func TestRunEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := freePort(t)
	o := transport.Options{Token: pairing.NewToken(), SNI: "www.example.com"}
	ctrl, err := control.Listen(net.JoinHostPort("127.0.0.1", strconv.Itoa(base)), o, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ctrl.Close()
	link := pairing.Link{Token: o.Token, Host: "127.0.0.1", Port: base, Pin: ctrl.Pin()}
	parsed, err := pairing.Parse(link.String())
	if err != nil || parsed.Token != o.Token {
		t.Fatalf("link round trip: %v %+v", err, parsed)
	}
	agent := &control.Agent{Addr: parsed.Addr(), Pin: parsed.Pin, Opts: o, Version: "test"}
	go func() { _ = agent.Run(ctx) }()
	hello, _, err := ctrl.WaitAgent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := runner.Run(ctx, runner.Config{
		Ctrl: ctrl, Opts: o, IranHost: "127.0.0.1", KharejHost: "127.0.0.1", BindHost: "127.0.0.1",
		PortBase: base, Methods: []string{"tcp", "wss-mux"}, Modes: []string{runner.Reverse, runner.Direct},
		ProbeTries: 2, BenchS: 1, Streams: 2, Soak: 3 * time.Second, SoakTop: 1,
	}, hello)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Rows) != 4 {
		t.Fatalf("want 4 rows, got %d", len(rep.Rows))
	}
	for _, r := range rep.Rows {
		s := r.Summary
		if s.Status != runner.StatusOK || s.DownMbps <= 0 || s.UpMbps <= 0 || s.Score <= 0 {
			t.Errorf("%s %s: %+v", r.Mode, r.Method, s)
		}
	}
	if rep.Rows[0].Soak == nil {
		t.Errorf("best row was not soaked")
	}
}

func TestScoreRanksFasterFirst(t *testing.T) {
	mk := func(name string, down float64) *runner.Row {
		return &runner.Row{Method: name, Summary: runner.Summary{Status: runner.StatusOK, DownMbps: down, UpMbps: down,
			FloorMbps: down / 2, RTTms: 50, Survival: 1}}
	}
	rows := []*runner.Row{mk("slow", 10), mk("fast", 100), {Method: "dead", Summary: runner.Summary{Status: runner.StatusDown}}}
	runner.ScoreAll(rows)
	if rows[0].Method != "fast" || rows[2].Method != "dead" || rows[2].Summary.Score != 0 {
		t.Fatalf("bad order: %s %s %s", rows[0].Method, rows[1].Method, rows[2].Method)
	}
}
