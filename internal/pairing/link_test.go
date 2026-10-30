package pairing

import "testing"

func TestLinkRoundTrip(t *testing.T) {
	in := Link{Token: NewToken(), Host: "194.61.3.99", Port: 20000, SNI: "www.speedtest.net", Pin: "q83vEjRWeJA-3q2-7_8AAQ"}
	out, err := Parse(in.String())
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
	v6 := Link{Token: "abc", Host: "2001:db8::1", Port: 443}
	if got, err := Parse(v6.String()); err != nil || got.Addr() != "[2001:db8::1]:443" {
		t.Fatalf("ipv6: %v %+v", err, got)
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{"http://x@1.2.3.4:1", "tb://1.2.3.4:1", "tb://tok@1.2.3.4", "tb://tok@1.2.3.4:99999"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}
