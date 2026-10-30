// Package runner drives a full benchmark from the Iran server: it decides
// which side listens and which dials for every method, runs the stages,
// and scores the results.
package runner

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/Art4Man/tunnelbench/internal/control"
	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/plan"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// Modes. Reverse = Kharej dials Iran (the usual tunnel setup); direct = Iran dials Kharej.
const (
	Reverse = "reverse"
	Direct  = "direct"
)

// Row is one method in one direction.
type Row struct {
	Mode    string               `json:"mode"`
	Method  string               `json:"method"`
	Family  string               `json:"family"`
	Port    int                  `json:"port"`
	Probe   *measure.ProbeResult `json:"probe,omitempty"`
	Down1   *measure.Transfer    `json:"down_1_stream,omitempty"` // Kharej -> Iran, what users download
	Up1     *measure.Transfer    `json:"up_1_stream,omitempty"`
	DownN   *measure.Transfer    `json:"down_n_streams,omitempty"`
	UpN     *measure.Transfer    `json:"up_n_streams,omitempty"`
	Soak    *measure.Soak        `json:"soak,omitempty"`
	Summary Summary              `json:"summary"`
}

// Config controls one run.
type Config struct {
	Ctrl       *control.Controller
	Opts       transport.Options
	IranHost   string // address the agent dials for reverse tests
	KharejHost string // address the runner dials for direct tests
	BindHost   string // local listen address, usually 0.0.0.0
	PortBase   int    // first test port; methods use the ports after it
	Methods    []string
	Modes      []string
	ProbeTries int
	BenchS     int
	Streams    int
	Soak       time.Duration
	SoakTop    int
	Progress   io.Writer
}

// Report is everything written to result.json.
type Report struct {
	Version    string        `json:"version"`
	Started    time.Time     `json:"started"`
	Finished   time.Time     `json:"finished"`
	IranHost   string        `json:"iran_host"`
	KharejHost string        `json:"kharej_host"`
	Agent      control.Hello `json:"agent"`
	BenchS     int           `json:"bench_s"`
	Streams    int           `json:"streams"`
	SoakS      int           `json:"soak_s"`
	Rows       []*Row        `json:"rows"`
}

func (c *Config) logf(f string, args ...any) {
	if c.Progress != nil {
		fmt.Fprintf(c.Progress, f, args...)
	}
}

// Run executes probe and benchmark for every method and mode, then soaks the best.
func Run(ctx context.Context, c Config, agent control.Hello) (*Report, error) {
	rep := &Report{Started: time.Now(), IranHost: c.IranHost, KharejHost: c.KharejHost, Agent: agent,
		BenchS: c.BenchS, Streams: c.Streams, SoakS: int(c.Soak / time.Second)}
	n := len(c.Methods)
	var rows []*Row
	// on any early return, keep and score what was measured so far
	defer func() {
		if rep.Rows == nil {
			for _, r := range rows {
				Summarize(r)
			}
			ScoreAll(rows)
			rep.Rows = rows
			rep.Finished = time.Now()
		}
	}()
	step := 0
	for mi, mode := range c.Modes {
		for i, name := range c.Methods {
			step++
			tr, ok := transport.Get(name)
			r := &Row{Mode: mode, Method: name, Port: c.PortBase + 1 + mi*n + i}
			rows = append(rows, r)
			if !ok {
				r.Summary.Note = "unknown method"
				continue
			}
			r.Family = tr.Family()
			if !slices.Contains(agent.Methods, name) {
				r.Summary.Note = "agent build lacks this method"
				c.logf("[%2d/%d] %-7s %-9s skipped: %s\n", step, n*len(c.Modes), mode, name, r.Summary.Note)
				continue
			}
			if tr.NeedsRoot() && (os.Geteuid() != 0 || !agent.Root) {
				r.Summary.Note = "needs root on both servers"
				continue
			}
			c.logf("[%2d/%d] %-7s %-9s probe… ", step, n*len(c.Modes), mode, name)
			res, err := c.stage(ctx, r, plan.Plan{Stage: plan.Probe, Method: name, Tries: c.ProbeTries})
			if err != nil {
				if ctx.Err() != nil {
					return rep, ctx.Err()
				}
				c.logf("error: %v\n", err)
				r.Summary.Note = err.Error()
				continue
			}
			r.Probe = res.Probe
			if r.Probe == nil || r.Probe.Connected == 0 {
				c.logf("DOWN\n")
				continue
			}
			c.logf("%d/%d ok, rtt %.1f ms · bench… ", r.Probe.Connected, r.Probe.Tries, median(r.Probe.RTTms))
			res, err = c.stage(ctx, r, plan.Plan{Stage: plan.Bench, Method: name, DurationS: c.BenchS, Streams: c.Streams})
			if err != nil {
				if ctx.Err() != nil {
					return rep, ctx.Err()
				}
				c.logf("error: %v\n", err)
				r.Summary.Note = err.Error()
				continue
			}
			c.mapBench(r, res)
			Summarize(r)
			c.logf("down %.1f / up %.1f Mbit/s\n", r.Summary.DownMbps, r.Summary.UpMbps)
		}
	}
	for _, r := range rows {
		Summarize(r)
	}
	ScoreAll(rows)

	if c.Soak > 0 && c.SoakTop > 0 {
		var top []*Row
		for _, r := range rows {
			if len(top) < c.SoakTop && r.Summary.Score > 0 {
				top = append(top, r)
			}
		}
		for i, r := range top {
			c.logf("[soak %d/%d] %-7s %-9s %s download… ", i+1, len(top), r.Mode, r.Method, c.Soak)
			res, err := c.stage(ctx, r, plan.Plan{Stage: plan.Soak, Method: r.Method, DurationS: int(c.Soak / time.Second),
				SoakPush: r.Mode == Reverse})
			if err != nil {
				if ctx.Err() != nil {
					return rep, ctx.Err()
				}
				c.logf("error: %v\n", err)
				continue
			}
			r.Soak = res.Soak
			c.logf("%d resets, up %.0f%% of the time\n", r.Soak.Resets, r.Soak.UpFraction*100)
		}
		for _, r := range rows {
			Summarize(r)
		}
		ScoreAll(rows)
	}
	rep.Rows = rows
	rep.Finished = time.Now()
	return rep, nil
}

// mapBench turns dialer-relative pull/push into user download/upload.
func (c *Config) mapBench(r *Row, res plan.Result) {
	if r.Mode == Reverse { // Kharej dials: pull = Iran->Kharej (upload), push = Kharej->Iran (download)
		r.Down1, r.Up1, r.DownN, r.UpN = res.Push1, res.Pull1, res.PushN, res.PullN
	} else { // Iran dials: pull = Kharej->Iran (download)
		r.Down1, r.Up1, r.DownN, r.UpN = res.Pull1, res.Push1, res.PullN, res.PushN
	}
}

func stageTimeout(p plan.Plan) time.Duration {
	switch p.Stage {
	case plan.Probe:
		return time.Duration(p.Tries)*25*time.Second + 45*time.Second
	case plan.Bench:
		return time.Duration(p.DurationS)*3*time.Second + 4*time.Minute
	default:
		return time.Duration(p.DurationS)*time.Second + 3*time.Minute
	}
}

// stage runs one plan with the right side listening and the other dialing.
func (c *Config) stage(ctx context.Context, r *Row, p plan.Plan) (plan.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, stageTimeout(p))
	defer cancel()
	port := strconv.Itoa(r.Port)
	if r.Mode == Reverse {
		tr, _ := transport.Get(r.Method)
		l, err := tr.Listen(ctx, net.JoinHostPort(c.BindHost, port), c.Opts)
		if err != nil {
			return plan.Result{}, fmt.Errorf("listen on Iran: %w", err)
		}
		defer l.Close()
		go control.Serve(l, c.Opts.Token)
		p.Addr = net.JoinHostPort(c.IranHost, port)
		resp, err := c.Ctrl.Call(ctx, control.Msg{Cmd: control.CmdTest, Plan: &p})
		if err != nil {
			return plan.Result{}, err
		}
		if resp.Result == nil {
			return plan.Result{}, fmt.Errorf("agent returned no result")
		}
		return *resp.Result, nil
	}
	addr := net.JoinHostPort("0.0.0.0", port)
	if _, err := c.Ctrl.Call(ctx, control.Msg{Cmd: control.CmdListen, Method: r.Method, Addr: addr}); err != nil {
		return plan.Result{}, fmt.Errorf("listen on Kharej: %w", err)
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = c.Ctrl.Call(cctx, control.Msg{Cmd: control.CmdClose, Addr: addr})
	}()
	p.Addr = net.JoinHostPort(c.KharejHost, port)
	res := plan.Execute(ctx, p, c.Opts)
	if res.Err != "" {
		return res, fmt.Errorf("%s", res.Err)
	}
	return res, nil
}
