//go:build !nokcp

package transport

import (
	"context"
	"net"

	kcp "github.com/xtaci/kcp-go/v5"
)

func init() {
	Register(kcpT{name: "kcp"})
	Register(kcpT{name: "kcp-fec", data: 10, parity: 3})
}

// kcpT is reliable, ordered transport over UDP, optionally with
// Reed-Solomon forward error correction (data:parity shards).
type kcpT struct {
	name         string
	data, parity int
}

func (k kcpT) Name() string  { return k.name }
func (kcpT) Family() string  { return "UDP" }
func (kcpT) Network() string { return "udp" }
func (kcpT) NeedsRoot() bool { return false }

func (k kcpT) block(o Options) (kcp.BlockCrypt, error) {
	return kcp.NewAESBlockCrypt(o.Key("kcp"))
}

// tune applies "fast" mode: nodelay, 10 ms interval, fast resend, no congestion window.
func tuneKCP(s *kcp.UDPSession) {
	s.SetStreamMode(true)
	s.SetWriteDelay(false)
	s.SetNoDelay(1, 10, 2, 1)
	s.SetWindowSize(1024, 1024)
	s.SetMtu(1350)
	s.SetACKNoDelay(true)
	_ = s.SetReadBuffer(4 << 20)
	_ = s.SetWriteBuffer(4 << 20)
}

type kcpListener struct{ *kcp.Listener }

func (l kcpListener) Accept() (net.Conn, error) {
	s, err := l.AcceptKCP()
	if err != nil {
		return nil, err
	}
	tuneKCP(s)
	return s, nil
}

func (k kcpT) Listen(_ context.Context, addr string, o Options) (Listener, error) {
	b, err := k.block(o)
	if err != nil {
		return nil, err
	}
	l, err := kcp.ListenWithOptions(addr, b, k.data, k.parity)
	if err != nil {
		return nil, err
	}
	_ = l.SetReadBuffer(8 << 20)
	_ = l.SetWriteBuffer(8 << 20)
	return kcpListener{l}, nil
}

func (k kcpT) Dial(_ context.Context, addr string, o Options) (net.Conn, error) {
	b, err := k.block(o)
	if err != nil {
		return nil, err
	}
	s, err := kcp.DialWithOptions(addr, b, k.data, k.parity)
	if err != nil {
		return nil, err
	}
	tuneKCP(s)
	return s, nil
}
