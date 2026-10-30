// Package report prints the result table and saves result.json.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Art4Man/tunnelbench/internal/runner"
)

const (
	reset  = "\033[0m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	dim    = "\033[2m"
	bold   = "\033[1m"
)

// UseColor reports whether w is a terminal that should get ANSI colours.
func UseColor(w io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func dot(status string, color bool) (string, string) {
	c, d := "", ""
	switch status {
	case runner.StatusOK:
		c, d = green, "●"
	case runner.StatusUnstable:
		c, d = yellow, "●"
	case runner.StatusDown:
		c, d = red, "●"
	default:
		c, d = dim, "○"
	}
	if !color {
		return d, ""
	}
	return c + d, reset
}

func num(v float64) string {
	if v <= 0 {
		return "-"
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

// Table writes the ranked result table.
func Table(w io.Writer, rep *runner.Report, color bool) {
	b := func(s string) string {
		if color {
			return bold + s + reset
		}
		return s
	}
	fmt.Fprintf(w, "\n%s  %s ⇄ %s  ·  bench %ds × %d streams", b("TUNNELBENCH"), rep.IranHost, rep.KharejHost, rep.BenchS, rep.Streams)
	if rep.SoakS > 0 {
		fmt.Fprintf(w, "  ·  soak %ds", rep.SoakS)
	}
	fmt.Fprintf(w, "\nDOWN = Kharej→Iran (what users download), UP = Iran→Kharej, Mbit/s\n\n")
	fmt.Fprintf(w, "%-3s %-2s %-9s %-8s %-9s %7s %7s %7s %7s %5s %6s %6s %6s  %s\n",
		"#", "", "STATUS", "MODE", "METHOD", "RTT ms", "DOWN", "UP", "FLOOR", "CV", "STALLS", "RESETS", "SCORE", "NOTE")
	fmt.Fprintln(w, strings.Repeat("─", 108))
	for i, r := range rep.Rows {
		s := r.Summary
		d, rs := dot(s.Status, color)
		rank := fmt.Sprint(i + 1)
		cv := "-"
		if s.DownMbps > 0 {
			cv = fmt.Sprintf("%.2f", s.CV)
		}
		resets := "-"
		if r.Soak != nil {
			resets = fmt.Sprint(s.Resets)
		}
		stalls := "-"
		if s.DownMbps > 0 {
			stalls = fmt.Sprint(s.Stalls)
		}
		score := "-"
		if s.Score > 0 {
			score = fmt.Sprintf("%.1f", s.Score)
		}
		note := s.Note
		if len(note) > 48 {
			note = note[:48] + "…"
		}
		fmt.Fprintf(w, "%-3s %s%s %-9s %-8s %-9s %7s %7s %7s %7s %5s %6s %6s %6s  %s\n",
			rank, d, rs, s.Status, r.Mode, r.Method, num(s.RTTms), num(s.DownMbps), num(s.UpMbps),
			num(s.FloorMbps), cv, stalls, resets, score, note)
	}
	fmt.Fprintln(w)
}

// Save writes result.json into dir and returns its path.
func Save(dir string, rep *runner.Report) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(dir, "result.json")
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", err
	}
	return p, os.WriteFile(p, b, 0o644)
}

// Load reads a saved result.json.
func Load(path string) (*runner.Report, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rep runner.Report
	return &rep, json.Unmarshal(b, &rep)
}
