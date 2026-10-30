// Package plan describes one test stage and runs it from the dialing side.
// Both the runner (Iran) and the agent (Kharej) execute plans, depending on
// which server dials in the direction being tested.
package plan

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// Stage names.
const (
	Probe = "probe"
	Bench = "bench"
	Soak  = "soak"
)

// Plan is sent over the control channel when the agent is the dialer.
type Plan struct {
	Stage     string `json:"stage"`
	Method    string `json:"method"`
	Addr      string `json:"addr"`
	Tries     int    `json:"tries,omitempty"`
	DurationS int    `json:"duration_s,omitempty"`
	Streams   int    `json:"streams,omitempty"`
	SoakPush  bool   `json:"soak_push,omitempty"`
}

// Result is what a dialer reports for one stage. Pull/Push are from the
// dialer's point of view; the runner maps them to download/upload.
type Result struct {
	Err   string               `json:"err,omitempty"`
	Probe *measure.ProbeResult `json:"probe,omitempty"`
	Pull1 *measure.Transfer    `json:"pull1,omitempty"`
	Push1 *measure.Transfer    `json:"push1,omitempty"`
	PullN *measure.Transfer    `json:"pulln,omitempty"`
	PushN *measure.Transfer    `json:"pushn,omitempty"`
	Soak  *measure.Soak        `json:"soak,omitempty"`
}

// Limits an agent enforces on plans it receives, so a misbehaving or
// impersonated runner cannot turn it into a long-running traffic source.
const (
	MaxDurationS = 3600
	MaxTries     = 100
	MaxStreams   = 16
)

// Validate rejects plans outside the limits.
func (p Plan) Validate() error {
	switch p.Stage {
	case Probe, Bench, Soak:
	default:
		return fmt.Errorf("unknown stage %q", p.Stage)
	}
	if p.DurationS < 0 || p.DurationS > MaxDurationS {
		return fmt.Errorf("duration %ds outside 0..%d", p.DurationS, MaxDurationS)
	}
	if p.Tries < 0 || p.Tries > MaxTries {
		return fmt.Errorf("tries %d outside 0..%d", p.Tries, MaxTries)
	}
	if p.Streams < 0 || p.Streams > MaxStreams {
		return fmt.Errorf("streams %d outside 0..%d", p.Streams, MaxStreams)
	}
	return nil
}

// Execute dials the method and runs the stage.
func Execute(ctx context.Context, p Plan, o transport.Options) Result {
	tr, ok := transport.Get(p.Method)
	if !ok {
		return Result{Err: "method not available in this build: " + p.Method}
	}
	if rs, ok := tr.(transport.Resetter); ok {
		rs.Reset(p.Addr)
		defer rs.Reset(p.Addr)
	}
	dial := func(ctx context.Context) (net.Conn, error) { return tr.Dial(ctx, p.Addr, o) }
	d := time.Duration(p.DurationS) * time.Second
	switch p.Stage {
	case Probe:
		r := measure.Probe(ctx, dial, o.Token, max(p.Tries, 1))
		return Result{Probe: &r}
	case Bench:
		n := max(p.Streams, 1)
		pull1 := measure.Pull(ctx, dial, o.Token, d, 1, 0)
		push1 := measure.Push(ctx, dial, o.Token, d, 1)
		half := max(d/2, 3*time.Second)
		pullN := measure.Pull(ctx, dial, o.Token, half, n, 0)
		pushN := measure.Push(ctx, dial, o.Token, half, n)
		return Result{Pull1: &pull1, Push1: &push1, PullN: &pullN, PushN: &pushN}
	case Soak:
		s := measure.RunSoak(ctx, dial, o.Token, d, p.SoakPush)
		return Result{Soak: &s}
	}
	return Result{Err: "unknown stage " + p.Stage}
}
