package measure

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DialFunc opens one test connection through the method under test.
type DialFunc func(ctx context.Context) (net.Conn, error)

// ProbeResult is the cheap first-stage check for one method.
type ProbeResult struct {
	Tries       int       `json:"tries"`
	Connected   int       `json:"connected"`
	HandshakeMs []float64 `json:"handshake_ms,omitempty"`
	RTTms       []float64 `json:"rtt_ms,omitempty"`
	BurstOK     bool      `json:"burst_ok"`
	BurstMbps   float64   `json:"burst_mbps,omitempty"`
	Errors      []string  `json:"errors,omitempty"`
}

// SuccessRate is the share of tries that connected and echoed.
func (p ProbeResult) SuccessRate() float64 {
	if p.Tries == 0 {
		return 0
	}
	return float64(p.Connected) / float64(p.Tries)
}

// explain turns low-level network errors into what they usually mean on a
// filtered route.
func explain(err error) string {
	var ne net.Error
	msg := err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout(),
		strings.Contains(msg, "no recent network activity"):
		return "no answer before the timeout (likely blocked)"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused (port closed or firewalled)"
	case errors.Is(err, syscall.ECONNRESET), strings.Contains(msg, "connection reset"):
		return "connection reset by the network (likely DPI)"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection closed mid-test (likely DPI)"
	}
	return msg
}

func (p *ProbeResult) addErr(err error) {
	s := explain(err)
	for _, e := range p.Errors {
		if e == s {
			return
		}
	}
	if len(p.Errors) < 5 {
		p.Errors = append(p.Errors, s)
	}
}

// Probe dials `tries` times, sends 5 echo pings on each connection, then
// pulls a 2 MB burst to catch filters that cut connections after a few packets.
func Probe(ctx context.Context, dial DialFunc, token string, tries int) ProbeResult {
	res := ProbeResult{Tries: tries}
	for i := 0; i < tries && ctx.Err() == nil; i++ {
		if i >= 3 && res.Connected == 0 {
			res.Tries = i // blocked: three straight failures are enough
			break
		}
		t0 := time.Now()
		c, err := dial(ctx)
		if err != nil {
			res.addErr(err)
			continue
		}
		hs := msSince(t0)
		rtts, err := echo(c, token, 5)
		c.Close()
		if err != nil {
			res.addErr(err)
			continue
		}
		res.Connected++
		res.HandshakeMs = append(res.HandshakeMs, hs)
		res.RTTms = append(res.RTTms, rtts...)
	}
	if res.Connected > 0 && ctx.Err() == nil {
		const burst = 2 << 20
		t0 := time.Now()
		tr := Pull(ctx, dial, token, 20*time.Second, 1, burst)
		if tr.Err == "" && tr.Bytes >= burst {
			res.BurstOK = true
			res.BurstMbps = float64(tr.Bytes) * 8 / 1e6 / time.Since(t0).Seconds()
		} else if tr.Err != "" {
			res.addErr(fmt.Errorf("2 MB burst: %s", tr.Err))
		}
	}
	return res
}

func echo(c net.Conn, token string, n int) ([]float64, error) {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.SetDeadline(time.Time{}) }()
	if err := writeHeader(c, token, Request{Op: OpEcho}); err != nil {
		return nil, err
	}
	ping := make([]byte, 64)
	pong := make([]byte, 64)
	var out []float64
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(ping, uint64(i))
		t0 := time.Now()
		if _, err := c.Write(ping); err != nil {
			return out, err
		}
		if _, err := io.ReadFull(c, pong); err != nil {
			return out, err
		}
		if binary.BigEndian.Uint64(pong) != uint64(i) {
			return out, fmt.Errorf("echo mismatch")
		}
		out = append(out, msSince(t0))
	}
	return out, nil
}

// Transfer is one throughput test, possibly over parallel streams.
type Transfer struct {
	Streams int       `json:"streams"`
	Mbps    []float64 `json:"mbps_per_second"`
	Bytes   int64     `json:"bytes"`
	Err     string    `json:"err,omitempty"`
	ErrAtS  float64   `json:"err_at_s,omitempty"`
}

func bucketsToMbps(b []int64) []float64 {
	out := make([]float64, len(b))
	for i, v := range b {
		out[i] = float64(v) * 8 / 1e6
	}
	return out
}

type firstErr struct {
	mu    sync.Mutex
	err   error
	at    float64
	start time.Time
}

func (f *firstErr) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err, f.at = err, time.Since(f.start).Seconds()
	}
}

// Pull measures listener -> dialer throughput.
func Pull(ctx context.Context, dial DialFunc, token string, d time.Duration, streams int, maxBytes int64) Transfer {
	if streams < 1 {
		streams = 1
	}
	start := time.Now()
	m := newMeter(start)
	fe := &firstErr{start: start}
	var total int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := dial(ctx)
			if err != nil {
				fe.set(err)
				return
			}
			defer c.Close()
			stop := context.AfterFunc(ctx, func() { c.Close() })
			defer stop()
			_ = c.SetDeadline(time.Now().Add(d + 30*time.Second))
			if err := writeHeader(c, token, Request{Op: OpPull, DurationMs: d.Milliseconds(), MaxBytes: maxBytes}); err != nil {
				fe.set(err)
				return
			}
			got, err := recvFrames(c, m)
			mu.Lock()
			total += got
			mu.Unlock()
			if err != nil {
				fe.set(err)
			}
		}()
	}
	wg.Wait()
	t := Transfer{Streams: streams, Mbps: bucketsToMbps(m.bytes()), Bytes: total}
	if fe.err != nil {
		t.Err, t.ErrAtS = shortErr(fe.err), fe.at
	}
	return t
}

// Push measures dialer -> listener throughput; the listener does the counting.
func Push(ctx context.Context, dial DialFunc, token string, d time.Duration, streams int) Transfer {
	if streams < 1 {
		streams = 1
	}
	gid := make([]byte, 8)
	_, _ = rand.Read(gid)
	group := hex.EncodeToString(gid)
	fe := &firstErr{start: time.Now()}
	var mu sync.Mutex
	var sum []int64
	var total int64
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := dial(ctx)
			if err != nil {
				fe.set(err)
				return
			}
			defer c.Close()
			stop := context.AfterFunc(ctx, func() { c.Close() })
			defer stop()
			_ = c.SetDeadline(time.Now().Add(d + 45*time.Second))
			if err := writeHeader(c, token, Request{Op: OpPush, DurationMs: d.Milliseconds(), Group: group}); err != nil {
				fe.set(err)
				return
			}
			if _, err := io.ReadFull(c, make([]byte, 1)); err != nil { // listener is ready
				fe.set(err)
				return
			}
			if _, err := sendFrames(c, d, 0); err != nil {
				fe.set(err)
				return
			}
			var rep pushReport
			if err := readJSON(c, &rep); err != nil {
				fe.set(fmt.Errorf("reading report: %w", err))
				return
			}
			mu.Lock()
			for len(sum) < len(rep.Buckets) {
				sum = append(sum, 0)
			}
			for j, v := range rep.Buckets {
				sum[j] += v
			}
			total += rep.Bytes
			mu.Unlock()
		}()
	}
	wg.Wait()
	t := Transfer{Streams: streams, Mbps: bucketsToMbps(sum), Bytes: total}
	if fe.err != nil {
		t.Err, t.ErrAtS = shortErr(fe.err), fe.at
	}
	return t
}

// Soak keeps one stream running for the whole duration, reconnecting after
// every failure, to expose duration-based kills and throttling.
type Soak struct {
	Mbps         []float64 `json:"mbps_per_second"`
	Resets       int       `json:"resets"`
	FirstResetS  float64   `json:"first_reset_s,omitempty"`
	UpFraction   float64   `json:"up_fraction"`
	ResetReasons []string  `json:"reset_reasons,omitempty"`
}

// RunSoak runs pull (push=false) or push transfers back to back.
func RunSoak(ctx context.Context, dial DialFunc, token string, total time.Duration, push bool) Soak {
	var s Soak
	secs := int(total / time.Second)
	s.Mbps = make([]float64, secs)
	start := time.Now()
	for ctx.Err() == nil {
		el := time.Since(start)
		rem := total - el
		if rem < 2*time.Second {
			break
		}
		off := int(el / time.Second)
		var tr Transfer
		if push {
			tr = Push(ctx, dial, token, rem, 1)
		} else {
			tr = Pull(ctx, dial, token, rem, 1, 0)
		}
		for i, v := range tr.Mbps {
			if off+i < secs {
				s.Mbps[off+i] += v
			}
		}
		if tr.Err != "" && time.Since(start) < total-2*time.Second {
			s.Resets++
			if s.Resets == 1 {
				s.FirstResetS = el.Seconds() + tr.ErrAtS
			}
			if len(s.ResetReasons) < 5 {
				s.ResetReasons = append(s.ResetReasons, tr.Err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	up := 0
	for _, v := range s.Mbps {
		if v > 0 {
			up++
		}
	}
	if secs > 0 {
		s.UpFraction = float64(up) / float64(secs)
	}
	return s
}

// Stats summarises per-second throughput samples.
type Stats struct {
	MedianMbps float64 `json:"median_mbps"`
	MeanMbps   float64 `json:"mean_mbps"`
	FloorMbps  float64 `json:"floor_mbps"` // 10th percentile second
	CV         float64 `json:"cv"`         // stddev / mean
	Stalls     int     `json:"stalls"`     // seconds below 10% of the 90th percentile
	Seconds    int     `json:"seconds"`
}

// Analyze drops the trailing partial second and the first (ramp-up)
// second, and summarises the steady state that remains.
func Analyze(mbps []float64) Stats {
	s := append([]float64(nil), mbps...)
	if len(s) > 2 {
		s = s[:len(s)-1]
	}
	if len(s) > 3 {
		s = s[1:]
	}
	st := Stats{Seconds: len(s)}
	if len(s) == 0 {
		return st
	}
	var sum float64
	for _, v := range s {
		sum += v
	}
	st.MeanMbps = sum / float64(len(s))
	sorted := append([]float64(nil), s...)
	sort.Float64s(sorted)
	st.MedianMbps = Percentile(sorted, 50)
	st.FloorMbps = Percentile(sorted, 10)
	var vs float64
	for _, v := range s {
		vs += (v - st.MeanMbps) * (v - st.MeanMbps)
	}
	if st.MeanMbps > 0 {
		st.CV = math.Sqrt(vs/float64(len(s))) / st.MeanMbps
	}
	// a stall is a second far below the good seconds (90th percentile), so a
	// transfer that is stuck most of the time still counts its stalls
	// (needs a few seconds of data to mean anything)
	if len(s) >= 4 {
		p90 := Percentile(sorted, 90)
		for _, v := range s {
			if v <= 0 || v < 0.1*p90 {
				st.Stalls++
			}
		}
	}
	return st
}

// Percentile expects sorted input; p in [0,100].
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p / 100 * float64(len(sorted)-1)
	lo := int(idx)
	if lo >= len(sorted)-1 {
		return sorted[len(sorted)-1]
	}
	f := idx - float64(lo)
	return sorted[lo]*(1-f) + sorted[lo+1]*f
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func shortErr(err error) string {
	s := explain(err)
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}
