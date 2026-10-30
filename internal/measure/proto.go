// Package measure implements the test protocol that runs inside every
// tunnel method: echo pings for latency and framed bulk transfers for
// throughput, sampled once per second at the receiving end.
package measure

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	magic     = "TBv1"
	frameSize = 32 << 10
	nonceLen  = 16
	macLen    = 32
)

// Op is what the dialer asks the listener to do on one connection.
type Op string

const (
	OpEcho Op = "echo" // listener echoes bytes back (latency pings)
	OpPull Op = "pull" // listener sends to dialer
	OpPush Op = "push" // dialer sends to listener; listener reports what it measured
)

// Request is sent by the dialer right after the connection opens.
type Request struct {
	Op         Op     `json:"op"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	MaxBytes   int64  `json:"max_bytes,omitempty"`
	Group      string `json:"group,omitempty"` // streams of one parallel test share a clock
}

var payload = func() []byte {
	b := make([]byte, frameSize)
	_, _ = rand.Read(b) // incompressible, so no method gets a free ride from compression
	return b
}()

// ErrAuth means the peer did not present the shared token.
var ErrAuth = errors.New("authentication failed")

func authKey(token string) []byte {
	h := sha256.Sum256([]byte("tunnelbench:auth:" + token))
	return h[:]
}

func writeHeader(w io.Writer, token string, req Request) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(magic)+nonceLen+macLen+2+len(body))
	buf = append(buf, magic...)
	nonce := make([]byte, nonceLen)
	_, _ = rand.Read(nonce)
	buf = append(buf, nonce...)
	m := hmac.New(sha256.New, authKey(token))
	m.Write(nonce)
	m.Write(body)
	buf = append(buf, m.Sum(nil)...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(body)))
	buf = append(buf, body...)
	_, err = w.Write(buf)
	return err
}

func readHeader(r io.Reader, token string) (Request, error) {
	var req Request
	head := make([]byte, len(magic)+nonceLen+macLen+2)
	if _, err := io.ReadFull(r, head); err != nil {
		return req, err
	}
	if string(head[:len(magic)]) != magic {
		return req, ErrAuth
	}
	nonce := head[len(magic) : len(magic)+nonceLen]
	mac := head[len(magic)+nonceLen : len(magic)+nonceLen+macLen]
	n := binary.BigEndian.Uint16(head[len(head)-2:])
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return req, err
	}
	m := hmac.New(sha256.New, authKey(token))
	m.Write(nonce)
	m.Write(body)
	if !hmac.Equal(mac, m.Sum(nil)) {
		return req, ErrAuth
	}
	return req, json.Unmarshal(body, &req)
}

// meter counts received payload bytes into one-second buckets.
type meter struct {
	start time.Time
	mu    sync.Mutex
	b     []int64
}

func newMeter(start time.Time) *meter { return &meter{start: start} }

func (m *meter) add(n int) {
	i := int(time.Since(m.start) / time.Second)
	m.mu.Lock()
	for len(m.b) <= i {
		m.b = append(m.b, 0)
	}
	m.b[i] += int64(n)
	m.mu.Unlock()
}

func (m *meter) bytes() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.b...)
}

// sendFrames writes frames until the deadline or byte cap, then an end frame.
func sendFrames(w io.Writer, d time.Duration, maxBytes int64) (int64, error) {
	var sent int64
	hdr := make([]byte, 4)
	end := time.Now().Add(d)
	for time.Now().Before(end) && (maxBytes == 0 || sent < maxBytes) {
		n := frameSize
		if maxBytes > 0 && maxBytes-sent < int64(n) {
			n = int(maxBytes - sent)
		}
		binary.BigEndian.PutUint32(hdr, uint32(n))
		if _, err := w.Write(hdr); err != nil {
			return sent, err
		}
		if _, err := w.Write(payload[:n]); err != nil {
			return sent, err
		}
		sent += int64(n)
	}
	binary.BigEndian.PutUint32(hdr, 0)
	_, err := w.Write(hdr)
	return sent, err
}

// recvFrames reads frames into the meter until the end frame.
func recvFrames(r io.Reader, m *meter) (int64, error) {
	var got int64
	hdr := make([]byte, 4)
	buf := make([]byte, frameSize)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return got, err
		}
		n := binary.BigEndian.Uint32(hdr)
		if n == 0 {
			return got, nil
		}
		if n > frameSize {
			return got, fmt.Errorf("bad frame size %d", n)
		}
		rem := int(n)
		for rem > 0 {
			k, err := r.Read(buf[:rem])
			if k > 0 {
				m.add(k)
				got += int64(k)
				rem -= k
			}
			if err != nil {
				return got, err
			}
		}
	}
}

func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	out := binary.BigEndian.AppendUint32(nil, uint32(len(b)))
	_, err = w.Write(append(out, b...))
	return err
}

func readJSON(r io.Reader, v any) error {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr)
	if n > 16<<20 {
		return fmt.Errorf("report too large")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// pushReport is what the listener sends back after a push.
type pushReport struct {
	Buckets []int64 `json:"buckets"`
	Bytes   int64   `json:"bytes"`
	Err     string  `json:"err,omitempty"`
}

// Server answers test connections on the listening side.
type Server struct {
	Token string

	mu     sync.Mutex
	groups map[string]time.Time
}

func (s *Server) groupStart(g string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groups == nil {
		s.groups = map[string]time.Time{}
	}
	now := time.Now()
	for k, t := range s.groups {
		if now.Sub(t) > 2*time.Hour {
			delete(s.groups, k)
		}
	}
	if g == "" {
		return now
	}
	if t, ok := s.groups[g]; ok {
		return t
	}
	s.groups[g] = now
	return now
}

// Serve handles one connection. It returns when the test ends.
func (s *Server) Serve(c net.Conn) {
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
	req, err := readHeader(c, s.Token)
	if err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	d := time.Duration(req.DurationMs) * time.Millisecond
	if d <= 0 || d > 2*time.Hour {
		d = 10 * time.Second
	}
	switch req.Op {
	case OpEcho:
		buf := make([]byte, 4096)
		for {
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := c.Read(buf)
			if n > 0 {
				if _, werr := c.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	case OpPull:
		_ = c.SetWriteDeadline(time.Now().Add(d + 30*time.Second))
		_, _ = sendFrames(c, d, req.MaxBytes)
		// wait for the dialer to finish reading before closing
		_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, _ = io.Copy(io.Discard, c)
	case OpPush:
		m := newMeter(s.groupStart(req.Group))
		// Tell the dialer we are reading before it sends bulk data. KCP has no
		// handshake: without this, the first burst can hit the session's default
		// window before Accept tunes it, and the sender idles until its 7 s probe.
		_ = c.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if _, err := c.Write([]byte{1}); err != nil {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(d + 30*time.Second))
		got, err := recvFrames(c, m)
		rep := pushReport{Buckets: m.bytes(), Bytes: got}
		if err != nil {
			rep.Err = err.Error()
			return // connection is broken; dialer will see the error itself
		}
		_ = c.SetWriteDeadline(time.Now().Add(15 * time.Second))
		_ = writeJSON(c, rep)
		_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
		_, _ = io.Copy(io.Discard, c)
	}
}
