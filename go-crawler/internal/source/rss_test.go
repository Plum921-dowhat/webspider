package source

import (
	"testing"
	"time"

	gofeed "github.com/mmcdole/gofeed"
)

func TestItemTimePrefersPublished(t *testing.T) {
	pub := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	upd := pub.Add(time.Hour)
	item := &gofeed.Item{PublishedParsed: &pub, UpdatedParsed: &upd}
	if got := itemTime(item); !got.Equal(pub) {
		t.Fatalf("want published time %v, got %v", pub, got)
	}
}

func TestItemTimeFallsBackToUpdated(t *testing.T) {
	upd := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	item := &gofeed.Item{UpdatedParsed: &upd}
	if got := itemTime(item); !got.Equal(upd) {
		t.Fatalf("want updated time %v, got %v", upd, got)
	}
}

func TestItemTimeZeroWhenAbsent(t *testing.T) {
	if got := itemTime(&gofeed.Item{}); !got.IsZero() {
		t.Fatalf("want zero time, got %v", got)
	}
}

func TestItemAuthorVariants(t *testing.T) {
	if got := itemAuthor(&gofeed.Item{Author: &gofeed.Person{Name: "a"}}); got != "a" {
		t.Fatalf("want a, got %q", got)
	}
	if got := itemAuthor(&gofeed.Item{Authors: []*gofeed.Person{{Name: "b"}}}); got != "b" {
		t.Fatalf("want b, got %q", got)
	}
	if got := itemAuthor(&gofeed.Item{}); got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}

func TestTruncate(t *testing.T) {
	if truncate("hello", 10) != "hello" || truncate("hello world", 5) != "hello" {
		t.Fatalf("truncate misbehaves")
	}
}
