package report

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Art4Man/tunnelbench/internal/runner"
)

func TestSaveUsesTimestampedName(t *testing.T) {
	start := time.Date(2026, 10, 30, 14, 35, 9, 0, time.FixedZone("IRST", 3*3600+1800))
	rep := &runner.Report{Started: start}
	dir := t.TempDir()
	p, err := Save(dir, rep)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "result-tb-20261030T110509Z.json"); p != want {
		t.Fatalf("saved to %s, want %s", p, want)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}
	back, err := Load(p)
	if err != nil || !back.Started.Equal(start) {
		t.Fatalf("round trip: %v %v", err, back.Started)
	}
}
