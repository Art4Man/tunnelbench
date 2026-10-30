package main

import "testing"

func TestShouldReplace(t *testing.T) {
	for _, c := range []struct {
		current, tag string
		force, want  bool
	}{
		{"0.1.0", "v0.1.1", false, true},
		{"0.1.0", "v0.1.0", false, false},
		{"0.1.0", "v0.1.0", true, false},
		{"0.2.0", "v0.1.9", false, false}, // never downgrade
		{"0.1.10", "v0.1.9", false, false},
		{"v0.1.0-3-gaa2cdc9", "v0.1.0", false, false}, // make build: keep
		{"aa2cdc9-dirty", "v0.1.0", false, false},
		{"dev", "v0.1.0", false, false},
		{"dev", "v0.1.0", true, true},
		{"0.1.0", "nightly", false, false},
		{"0.1.0", "v0.1.1-rc.1", false, true}, // release candidates
		{"0.1.1-rc.1", "v0.1.1-rc.2", false, true},
		{"0.1.1-rc.2", "v0.1.1", false, true},
		{"0.1.1-rc.9", "v0.1.1-rc.10", false, true},
		{"0.1.1", "v0.1.1-rc.3", false, false},
		{"0.1.1-rc.1", "v0.1.1-rc.1", false, false},
		{"v0.1.1-rc.1-3-gaa2cdc9", "v0.1.1", false, false}, // make build after an rc tag
		{"0.1.1-rc.x", "v0.1.1", false, false},
	} {
		if got, _ := shouldReplace(c.current, c.tag, c.force); got != c.want {
			t.Errorf("shouldReplace(%q, %q, %v) = %v, want %v", c.current, c.tag, c.force, got, c.want)
		}
	}
}
