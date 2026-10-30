package runner

import (
	"fmt"
	"math"
	"sort"

	"github.com/Art4Man/tunnelbench/internal/measure"
)

// Status values shown in the report.
const (
	StatusOK       = "OK"
	StatusUnstable = "UNSTABLE"
	StatusDown     = "DOWN"
	StatusSkipped  = "SKIPPED"
)

// Weights of the score terms; they sum to 1.
var Weights = struct{ Down, Up, Floor, Stability, Latency float64 }{
	Down: 0.30, Up: 0.15, Floor: 0.20, Stability: 0.20, Latency: 0.15,
}

// Summary is the per-row numbers the score and table use.
type Summary struct {
	RTTms        float64 `json:"rtt_ms"`
	ProbeSuccess float64 `json:"probe_success"`
	DownMbps     float64 `json:"down_mbps"`    // median, parallel streams, Kharej -> Iran
	UpMbps       float64 `json:"up_mbps"`      // median, parallel streams, Iran -> Kharej
	Down1Mbps    float64 `json:"down_1s_mbps"` // median, single stream
	FloorMbps    float64 `json:"floor_mbps"`   // 10th percentile second, single-stream download
	CV           float64 `json:"cv"`           // variability of single-stream download
	Stalls       int     `json:"stalls"`
	Resets       int     `json:"resets"`
	FirstResetS  float64 `json:"first_reset_s,omitempty"`
	Survival     float64 `json:"survival"`
	Score        float64 `json:"score"`
	Status       string  `json:"status"`
	Note         string  `json:"note,omitempty"`
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return measure.Percentile(s, 50)
}

// Summarize fills a row's summary from its stage results.
func Summarize(r *Row) {
	s := &r.Summary
	if p := r.Probe; p != nil {
		s.ProbeSuccess = p.SuccessRate()
		s.RTTms = median(p.RTTms)
	}
	if r.Down1 != nil {
		st := measure.Analyze(r.Down1.Mbps)
		s.Down1Mbps, s.FloorMbps, s.CV = st.MedianMbps, st.FloorMbps, st.CV
	}
	if r.DownN != nil {
		s.DownMbps = measure.Analyze(r.DownN.Mbps).MedianMbps
	}
	// stalls in any of the four transfers: users notice upload stalls too
	s.Stalls = 0
	for _, t := range []*measure.Transfer{r.Down1, r.Up1, r.DownN, r.UpN} {
		if t != nil {
			s.Stalls += measure.Analyze(t.Mbps).Stalls
		}
	}
	if r.UpN != nil {
		s.UpMbps = measure.Analyze(r.UpN.Mbps).MedianMbps
	}
	s.DownMbps = math.Max(s.DownMbps, s.Down1Mbps)
	if r.UpN == nil && r.Up1 != nil {
		s.UpMbps = measure.Analyze(r.Up1.Mbps).MedianMbps
	}
	s.Survival = 1
	benchErr := false
	for _, t := range []*measure.Transfer{r.Down1, r.Up1, r.DownN, r.UpN} {
		if t != nil && t.Err != "" {
			benchErr = true
			if s.Note == "" {
				s.Note = t.Err
			}
		}
	}
	if benchErr {
		s.Survival = 0.5
	}
	if k := r.Soak; k != nil {
		s.Resets, s.FirstResetS, s.Survival = k.Resets, k.FirstResetS, k.UpFraction
		st := measure.Analyze(k.Mbps)
		s.Stalls = st.Stalls
		s.CV = st.CV
		s.FloorMbps = st.FloorMbps
	}

	// A method that moves data in only one direction, or collapses in one
	// direction, is not usable for a VPN however fast the other side is.
	collapse := ""
	if r.Down1 != nil || r.DownN != nil || r.Up1 != nil || r.UpN != nil {
		switch {
		case s.DownMbps <= 0:
			collapse = "no data Kharej→Iran (download)"
		case s.UpMbps <= 0:
			collapse = "no data Iran→Kharej (upload)"
		case s.DownMbps < 0.1*s.UpMbps:
			collapse = fmt.Sprintf("download collapsed: %.0f vs %.0f Mbit/s up", s.DownMbps, s.UpMbps)
		case s.UpMbps < 0.1*s.DownMbps:
			collapse = fmt.Sprintf("upload collapsed: %.0f vs %.0f Mbit/s down", s.UpMbps, s.DownMbps)
		}
	}

	switch {
	case r.Probe == nil:
		s.Status = StatusSkipped
	case r.Probe.Connected == 0:
		s.Status = StatusDown
		if len(r.Probe.Errors) > 0 {
			s.Note = r.Probe.Errors[0]
		}
	case s.ProbeSuccess < 0.9 || !r.Probe.BurstOK || benchErr || s.Resets > 0 || s.Survival < 0.9 ||
		collapse != "" || s.Stalls > 0:
		s.Status = StatusUnstable
		switch {
		case s.Note != "":
		case collapse != "":
			s.Note = collapse
		case !r.Probe.BurstOK:
			s.Note = "dies after a few packets (2 MB burst failed)"
		case s.Stalls > 0:
			s.Note = fmt.Sprintf("%d stalled seconds", s.Stalls)
		}
	default:
		s.Status = StatusOK
	}
}

// ScoreAll scores every working row against the best row in the same run.
func ScoreAll(rows []*Row) {
	var bestDown, bestUp, bestFloor, bestRTT float64
	bestRTT = math.Inf(1)
	for _, r := range rows {
		s := r.Summary
		if s.Status == StatusDown || s.Status == StatusSkipped {
			continue
		}
		bestDown = math.Max(bestDown, s.DownMbps)
		bestUp = math.Max(bestUp, s.UpMbps)
		bestFloor = math.Max(bestFloor, s.FloorMbps)
		if s.RTTms > 0 {
			bestRTT = math.Min(bestRTT, s.RTTms)
		}
	}
	norm := func(v, best float64) float64 {
		if best <= 0 {
			return 0
		}
		return math.Min(v/best, 1)
	}
	for _, r := range rows {
		s := &r.Summary
		if s.Status == StatusDown || s.Status == StatusSkipped {
			s.Score = 0
			continue
		}
		lat := 0.0
		if s.RTTms > 0 && !math.IsInf(bestRTT, 1) {
			lat = bestRTT / s.RTTms
		}
		stab := math.Max(0, 1-s.CV) * s.Survival
		w := Weights
		s.Score = 100 * (w.Down*norm(s.DownMbps, bestDown) + w.Up*norm(s.UpMbps, bestUp) +
			w.Floor*norm(s.FloorMbps, bestFloor) + w.Stability*stab + w.Latency*lat)
		s.Score = math.Round(s.Score*10) / 10
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, rj := statusRank(rows[i].Summary.Status), statusRank(rows[j].Summary.Status)
		if ri != rj {
			return ri < rj
		}
		return rows[i].Summary.Score > rows[j].Summary.Score
	})
}

func statusRank(s string) int {
	switch s {
	case StatusOK:
		return 0
	case StatusUnstable:
		return 1
	case StatusDown:
		return 2
	}
	return 3
}
