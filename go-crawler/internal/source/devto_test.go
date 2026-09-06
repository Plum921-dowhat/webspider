package source

import (
	"net/http"
	"testing"
	"time"
)

func TestParseTagsStringArray(t *testing.T) {
	got := parseTags([]byte(`["go","redis"]`))
	if len(got) != 2 || got[0] != "go" || got[1] != "redis" {
		t.Fatalf("want [go redis], got %v", got)
	}
}

func TestParseTagsObjectArray(t *testing.T) {
	got := parseTags([]byte(`[{"name":"go"},{"name":""},{"name":"redis"}]`))
	if len(got) != 2 || got[0] != "go" || got[1] != "redis" {
		t.Fatalf("want [go redis] with empty names dropped, got %v", got)
	}
}

func TestParseTagsEmptyAndInvalid(t *testing.T) {
	if got := parseTags(nil); got != nil {
		t.Fatalf("nil input should give nil, got %v", got)
	}
	if got := parseTags([]byte(`42`)); got != nil {
		t.Fatalf("unparseable input should give nil, got %v", got)
	}
}

func TestParseRetryAfterDeltaSeconds(t *testing.T) {
	if d := parseRetryAfter("30"); d != 30*time.Second {
		t.Fatalf("want 30s, got %v", d)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	future := time.Now().Add(2 * time.Minute).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(future); d <= 0 {
		t.Fatalf("want positive duration for future date %q, got %v", future, d)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(past); d != 0 {
		t.Fatalf("past date should clamp to 0, got %v", d)
	}
}

func TestParseRetryAfterAbsent(t *testing.T) {
	for _, v := range []string{"", "soon", "-5"} {
		if d := parseRetryAfter(v); d != 0 {
			t.Fatalf("input %q should give 0, got %v", v, d)
		}
	}
}

func TestBackoffSecondsCapped(t *testing.T) {
	if got := backoffSeconds(0); got != time.Second {
		t.Fatalf("attempt 0 should be 1s, got %v", got)
	}
	if got := backoffSeconds(7); got != 32*time.Second {
		t.Fatalf("attempt >=5 should cap at 32s, got %v", got)
	}
}
