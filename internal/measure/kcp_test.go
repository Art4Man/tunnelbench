package measure_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Art4Man/tunnelbench/internal/measure"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// TestPushKCPLateAccept reproduces the reverse-KCP stalls seen on a real route:
// the first push segments reach the listener before Accept tunes the session,
// fill kcp-go's default 32-segment window and get acked with window 0. Without
// a reply from the listener before bulk data, the sender then idles until
// KCP's 7 s window probe.
func TestPushKCPLateAccept(t *testing.T) {
	tr, ok := transport.Get("kcp")
	if !ok {
		t.Skip("kcp not in this build")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	o := transport.Options{Token: "test-token"}
	l, err := tr.Listen(ctx, "127.0.0.1:0", o)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := &measure.Server{Token: o.Token}
	go func() {
		for {
			time.Sleep(300 * time.Millisecond) // a busy listener: data arrives before Accept
			c, err := l.Accept()
			if err != nil {
				return
			}
			go srv.Serve(c)
		}
	}()
	dial := func(ctx context.Context) (net.Conn, error) { return tr.Dial(ctx, l.Addr().String(), o) }

	t0 := time.Now()
	res := measure.Push(ctx, dial, o.Token, 2*time.Second, 1)
	if res.Err != "" {
		t.Fatal(res.Err)
	}
	if took := time.Since(t0); took > 5*time.Second {
		t.Errorf("2 s push took %v; per-second Mbit/s %v", took.Round(time.Millisecond), res.Mbps)
	}
}
