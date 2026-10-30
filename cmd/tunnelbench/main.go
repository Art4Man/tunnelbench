// Command tunnelbench benchmarks tunnel methods between an Iran server and a
// server abroad ("Kharej") and ranks them by speed and stability.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Art4Man/tunnelbench/internal/control"
	"github.com/Art4Man/tunnelbench/internal/pairing"
	"github.com/Art4Man/tunnelbench/internal/report"
	"github.com/Art4Man/tunnelbench/internal/runner"
	"github.com/Art4Man/tunnelbench/internal/transport"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `tunnelbench — find the fastest, steadiest tunnel between two servers

Usage:
  tunnelbench run   [flags]        on the Iran server: print a link, wait for Kharej, run the benchmark
  tunnelbench join                 on the Kharej server: paste the link, then it serves tests
  tunnelbench local [flags]        run every method over loopback on this machine (self-test)
  tunnelbench show  [-rescore] <result-tb-….json>  print a saved result again
  tunnelbench methods              list the methods in this build
  tunnelbench update [-pre]        replace this binary with the latest release, or with -pre the latest
                                   release candidate (run on both servers)
  tunnelbench version

Run "tunnelbench run -h" for the benchmark flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(ctx, os.Args[2:], false)
	case "local":
		err = cmdRun(ctx, os.Args[2:], true)
	case "join", "agent":
		err = cmdJoin(ctx, os.Args[2:])
	case "show":
		err = cmdShow(os.Args[2:])
	case "update":
		err = cmdUpdate(os.Args[2:])
	case "methods":
		for _, t := range transport.All() {
			root := ""
			if t.NeedsRoot() {
				root = " (root)"
			}
			fmt.Printf("%-10s %-10s %s%s\n", t.Name(), t.Family(), t.Network(), root)
		}
	case "version", "-v", "--version":
		fmt.Println("tunnelbench", version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func allMethods() []string {
	var out []string
	for _, t := range transport.All() {
		out = append(out, t.Name())
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// outboundIP guesses this server's public address from its default route.
func outboundIP() string {
	c, err := net.Dial("udp", "1.1.1.1:53")
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

func cmdRun(ctx context.Context, args []string, local bool) error {
	name := "run"
	if local {
		name = "local"
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	listen := fs.String("listen", "0.0.0.0:20000", "control address; test ports are the ones right after it")
	publicIP := fs.String("public-ip", "", "this (Iran) server's address as seen from Kharej (default: auto-detect)")
	kharejIP := fs.String("kharej-ip", "", "Kharej server address for direct tests (default: where the agent connected from)")
	methods := fs.String("methods", strings.Join(allMethods(), ","), "comma-separated methods to test")
	modes := fs.String("modes", "reverse,direct", "reverse (Kharej dials Iran), direct (Iran dials Kharej), or both")
	bench := fs.Duration("bench", 20*time.Second, "length of each single-stream transfer (parallel transfers run half as long)")
	streams := fs.Int("streams", 4, "parallel streams for the multi-stream transfers")
	tries := fs.Int("tries", 10, "connections opened in the probe stage")
	soak := fs.Duration("soak", 5*time.Minute, "soak length for the best methods (0 disables)")
	soakTop := fs.Int("soak-top", 3, "how many of the best methods get a soak")
	quick := fs.Bool("quick", false, "short run: 8s transfers, 5 probes, no soak")
	sni := fs.String("sni", "www.speedtest.net", "SNI for TLS, WSS and QUIC methods")
	out := fs.String("out", "", "directory for the result-tb-<timestamp>.json file (default ./tunnelbench-runs/<time>)")
	_ = fs.Parse(args)

	if *quick {
		*bench, *tries, *soak = 8*time.Second, 5, 0
	}
	host, portS, err := net.SplitHostPort(*listen)
	if err != nil {
		return fmt.Errorf("bad -listen: %w", err)
	}
	base, err := strconv.Atoi(portS)
	if err != nil {
		return fmt.Errorf("bad -listen port: %w", err)
	}
	if local {
		host, *publicIP, *kharejIP = "127.0.0.1", "127.0.0.1", "127.0.0.1"
		*listen = net.JoinHostPort(host, portS)
	}
	if *publicIP == "" {
		*publicIP = outboundIP()
		if *publicIP == "" {
			return errors.New("could not detect this server's IP; pass -public-ip")
		}
	}

	o := transport.Options{Token: pairing.NewToken(), SNI: *sni}
	warn := func(msg string) { fmt.Fprintf(os.Stderr, "\nWARNING: %s\n", msg) }
	ctrl, err := control.Listen(*listen, o, warn)
	if err != nil {
		return fmt.Errorf("control listener: %w", err)
	}
	defer ctrl.Close()
	link := pairing.Link{Token: o.Token, Host: *publicIP, Port: base, SNI: *sni, Pin: ctrl.Pin()}
	ml := splitList(*methods)
	md := splitList(*modes)
	last := base + len(ml)*len(md)

	if local {
		a := &control.Agent{Addr: link.Addr(), Pin: link.Pin, Opts: o, Version: version}
		go func() { _ = a.Run(ctx) }()
	} else {
		fmt.Printf("\nOn the Kharej server run \"tunnelbench join\" and paste this one-time link when asked:\n\n  %s\n\n", link)
		fmt.Println("The link works only for this run and only for the first agent that joins; keep it private.")
		fmt.Printf("Open these ports on both servers (TCP and UDP): %d-%d\nWaiting for the Kharej agent… (Ctrl+C to cancel)\n", base, last)
	}
	hello, from, err := ctrl.WaitAgent(ctx)
	if err != nil {
		return err
	}
	if *kharejIP == "" {
		*kharejIP, _, _ = net.SplitHostPort(from.String())
	}
	fmt.Printf("Agent joined: %s (%s, version %s, root=%v)\n\n", hello.Hostname, *kharejIP, hello.Version, hello.Root)
	if hello.Version != version {
		warn(fmt.Sprintf("the Kharej agent runs version %s but this server runs %s; update both so results are comparable", hello.Version, version))
	}

	cfg := runner.Config{
		Ctrl: ctrl, Opts: o, IranHost: *publicIP, KharejHost: *kharejIP, BindHost: host,
		PortBase: base, Methods: ml, Modes: md, ProbeTries: *tries,
		BenchS: int(*bench / time.Second), Streams: *streams, Soak: *soak, SoakTop: *soakTop,
		Progress: os.Stdout,
	}
	rep, runErr := runner.Run(ctx, cfg, hello)
	if rep != nil {
		rep.Version = version
		if len(rep.Rows) > 0 {
			report.Table(os.Stdout, rep, report.UseColor(os.Stdout))
		}
		dir := *out
		if dir == "" {
			dir = filepath.Join("tunnelbench-runs", time.Now().Format("2006-01-02_150405"))
		}
		if p, err := report.Save(dir, rep); err == nil {
			fmt.Println("Saved", p)
		} else {
			fmt.Fprintln(os.Stderr, "could not save result:", err)
		}
	}
	return runErr
}

func cmdJoin(ctx context.Context, args []string) error {
	var raw string
	switch len(args) {
	case 0:
		fmt.Fprint(os.Stderr, "Paste the link from the Iran server: ")
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return fmt.Errorf("reading link: %w", err)
		}
		raw = strings.TrimSpace(line)
	case 1:
		raw = args[0]
		fmt.Fprintln(os.Stderr, "Tip: run \"tunnelbench join\" without arguments and paste the link, so it stays out of shell history and ps.")
	default:
		return errors.New("usage: tunnelbench join   (then paste the tb:// link)")
	}
	link, err := pairing.Parse(strings.Trim(raw, "'\""))
	if err != nil {
		return err
	}
	a := &control.Agent{
		Pin:     link.Pin,
		Addr:    link.Addr(),
		Opts:    transport.Options{Token: link.Token, SNI: link.SNI},
		Version: version,
		Logf: func(f string, v ...any) {
			fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(f, v...))
		},
	}
	fmt.Printf("Joining %s … (Ctrl+C to stop)\n", link.Addr())
	return a.Run(ctx)
}

func cmdShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	rescore := fs.Bool("rescore", false, "recompute status and score with this version's rules")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: tunnelbench show [-rescore] <result-tb-….json>")
	}
	rep, err := report.Load(fs.Arg(0))
	if err != nil {
		return err
	}
	if *rescore {
		for _, r := range rep.Rows {
			r.Summary = runner.Summary{}
			runner.Summarize(r)
		}
		runner.ScoreAll(rep.Rows)
	}
	report.Table(os.Stdout, rep, report.UseColor(os.Stdout))
	return nil
}
