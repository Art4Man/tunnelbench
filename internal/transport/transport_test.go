package transport_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// TestLoopback runs every non-root method end to end over 127.0.0.1.
func TestLoopback(t *testing.T) {
	const token = "test-token"
	o := transport.Options{Token: token, SNI: "www.example.com"}
	for _, tr := range transport.All() {
		if tr.NeedsRoot() {
			continue
		}
		t.Run(tr.Name(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			l, err := tr.Listen(ctx, "127.0.0.1:0", o)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer l.Close()
			srv := &measure.Server{Token: token}
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					go srv.Serve(c)
				}
			}()
			addr := l.Addr().String()
			dial := func(ctx context.Context) (net.Conn, error) { return tr.Dial(ctx, addr, o) }

			p := measure.Probe(ctx, dial, token, 3)
			if p.Connected != 3 || !p.BurstOK {
				t.Fatalf("probe: %+v", p)
			}
			pull := measure.Pull(ctx, dial, token, 2*time.Second, 2, 0)
			if pull.Err != "" || pull.Bytes == 0 {
				t.Fatalf("pull: err=%q bytes=%d", pull.Err, pull.Bytes)
			}
			push := measure.Push(ctx, dial, token, 2*time.Second, 2)
			if push.Err != "" || push.Bytes == 0 {
				t.Fatalf("push: err=%q bytes=%d", push.Err, push.Bytes)
			}
			t.Logf("rtt=%.2fms pull=%.0f Mbit/s push=%.0f Mbit/s",
				p.RTTms[0], measure.Analyze(pull.Mbps).MedianMbps, measure.Analyze(push.Mbps).MedianMbps)
		})
	}
}

func TestBadTokenRejected(t *testing.T) {
	tr, _ := transport.Get("tcp")
	ctx := context.Background()
	l, err := tr.Listen(ctx, "127.0.0.1:0", transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := &measure.Server{Token: "right"}
	go func() {
		c, err := l.Accept()
		if err == nil {
			srv.Serve(c)
		}
	}()
	dial := func(ctx context.Context) (net.Conn, error) {
		return tr.Dial(ctx, l.Addr().String(), transport.Options{})
	}
	p := measure.Probe(ctx, dial, "wrong", 1)
	if p.Connected != 0 {
		t.Fatalf("wrong token was accepted: %+v", p)
	}
}
