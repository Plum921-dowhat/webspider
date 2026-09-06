package stream

import "testing"

func TestHashURLStableAndDistinct(t *testing.T) {
	a := HashURL("https://dev.to/post-1")
	b := HashURL("https://dev.to/post-1")
	c := HashURL("https://dev.to/post-2")

	if a != b {
		t.Fatalf("same URL must hash identically: %s vs %s", a, b)
	}
	if a == c {
		t.Fatalf("different URLs must hash differently")
	}
	if len(a) != 64 {
		t.Fatalf("sha256 hex must be 64 chars, got %d", len(a))
	}
	for _, r := range a {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Fatalf("hash must be lowercase hex, got %q", a)
		}
	}
}
