package measure

import (
	"math"
	"testing"
)

func TestAnalyze(t *testing.T) {
	// last second is partial and must be ignored
	st := Analyze([]float64{100, 100, 100, 5, 100, 100, 1})
	if st.Seconds != 5 {
		t.Fatalf("seconds = %d, want 5 (partial last and ramp-up first second dropped)", st.Seconds)
	}
	if st.MedianMbps != 100 {
		t.Errorf("median = %v, want 100", st.MedianMbps)
	}
	if st.Stalls != 1 {
		t.Errorf("stalls = %d, want 1", st.Stalls)
	}
	if st.CV <= 0 || st.FloorMbps >= 100 {
		t.Errorf("cv=%v floor=%v: variability not detected", st.CV, st.FloorMbps)
	}
	steady := Analyze([]float64{50, 50, 50, 50, 10})
	if steady.CV != 0 || math.Abs(steady.FloorMbps-50) > 1e-9 {
		t.Errorf("steady run: %+v", steady)
	}
}

func TestAnalyzeMostlyStuck(t *testing.T) {
	// the reverse kcp-fec pattern from a real run: stuck, then one burst
	st := Analyze([]float64{0.3, 0, 0, 0, 0, 0, 0, 84, 0.5})
	if st.Stalls != 6 || st.MedianMbps != 0 {
		t.Fatalf("stuck transfer not detected: %+v", st)
	}
}

func TestPercentile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5}
	if p := Percentile(s, 50); p != 3 {
		t.Errorf("p50 = %v", p)
	}
	if p := Percentile(s, 100); p != 5 {
		t.Errorf("p100 = %v", p)
	}
	if p := Percentile(nil, 50); p != 0 {
		t.Errorf("empty = %v", p)
	}
}
