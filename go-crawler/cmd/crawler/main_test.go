package main

import (
	"testing"
	"time"
)

func base() time.Time { return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC) }

func TestResolveSinceWithoutCursorFallsBackToWindow(t *testing.T) {
	cursor := base().Add(-30 * time.Hour)
	got := resolveSince(base(), cursor, false, 5*time.Minute)
	if !got.Equal(base()) {
		t.Fatalf("no cursor should yield config window base, got %v", got)
	}
}

func TestResolveSinceCursorNewerWinsWithOverlap(t *testing.T) {
	cursor := base().Add(time.Hour) // cursor well inside the 24h window
	got := resolveSince(base(), cursor, true, 5*time.Minute)
	want := cursor.Add(-5 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("want cursor-overlap %v, got %v", want, got)
	}
}

func TestResolveSinceOlderCursorKeepsWindow(t *testing.T) {
	// cursor older than the config window (e.g. Redis state cleared then
	// repopulated stale) must not widen the window beyond the base
	cursor := base().Add(-48 * time.Hour)
	got := resolveSince(base(), cursor, true, 5*time.Minute)
	if !got.Equal(base()) {
		t.Fatalf("stale cursor should yield base, got %v", got)
	}
}

func TestResolveSinceZeroBaseUsesCursor(t *testing.T) {
	// since_hours unset (base zero time): cursor alone drives the window
	cursor := base().Add(-2 * time.Hour)
	got := resolveSince(time.Time{}, cursor, true, 5*time.Minute)
	if !got.Equal(cursor.Add(-5 * time.Minute)) {
		t.Fatalf("zero base should take cursor, got %v", got)
	}
}
