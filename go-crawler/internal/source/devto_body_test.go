package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"en-tech-pipeline/go-crawler/internal/config"
)

func boolPtr(b bool) *bool { return &b }

func latestItemJSON(id int) string {
	return `[{"id":` + itoa(id) + `,"url":"https://dev.to/x/post-` + itoa(id) + `","title":"T",` +
		`"published_at":"2026-09-13T00:00:00Z","tag_list":["go"],"user":{"name":"a"}}]`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}

// bodyServer spins up an httptest server imitating /latest (no body fields)
// and /articles/{id} (body_markdown), pointing the package vars at it.
func bodyServer(t *testing.T, articleStatus int, withBodyInList bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	list := latestItemJSON(7)
	if withBodyInList {
		list = `[{"id":7,"url":"https://dev.to/x/post-7","title":"T","published_at":"2026-09-13T00:00:00Z","body_markdown":"# Inline","user":{"name":"a"}}]`
	}
	mux.HandleFunc("/latest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(list))
	})
	mux.HandleFunc("/articles/7", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(articleStatus)
		if articleStatus == http.StatusOK {
			_, _ = w.Write([]byte(`{"body_markdown":"# Fetched body"}`))
		}
	})
	srv := httptest.NewServer(mux)
	oldList, oldAPI := devToBase, devToArticleBase
	devToBase, devToArticleBase = srv.URL+"/latest", srv.URL+"/articles"
	t.Cleanup(func() {
		devToBase, devToArticleBase = oldList, oldAPI
		srv.Close()
	})
	return srv
}

func TestFetchSinceFetchesBodyPerArticle(t *testing.T) {
	bodyServer(t, http.StatusOK, false)
	src := NewDevTo(config.CrawlerConfig{FetchBody: boolPtr(true)})
	arts, _, empty, err := src.FetchSince(context.Background(), 1, 10, time.Time{})
	if err != nil || empty || len(arts) != 1 {
		t.Fatalf("fetch failed: err=%v empty=%v n=%d", err, empty, len(arts))
	}
	if arts[0].ContentMD != "# Fetched body" {
		t.Fatalf("want per-article body, got %q", arts[0].ContentMD)
	}
}

func TestFetchSinceUsesInlineBodyWhenListProvidesIt(t *testing.T) {
	bodyServer(t, http.StatusOK, true)
	src := NewDevTo(config.CrawlerConfig{FetchBody: boolPtr(true)})
	arts, _, _, err := src.FetchSince(context.Background(), 1, 10, time.Time{})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if arts[0].ContentMD != "# Inline" {
		t.Fatalf("want inline list body, got %q", arts[0].ContentMD)
	}
}

func TestFetchSinceBodyDisabledLeavesContentEmpty(t *testing.T) {
	bodyServer(t, http.StatusOK, false)
	src := NewDevTo(config.CrawlerConfig{FetchBody: boolPtr(false)})
	arts, _, _, err := src.FetchSince(context.Background(), 1, 10, time.Time{})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if arts[0].ContentMD != "" {
		t.Fatalf("fetch_body=false must leave ContentMD empty, got %q", arts[0].ContentMD)
	}
}

func TestFetchSinceBodyNotFoundFallsBackToWebPath(t *testing.T) {
	// 404 on the per-article call must not fail the page: ContentMD stays
	// empty and the processor's web-fetch path makes the final call
	bodyServer(t, http.StatusNotFound, false)
	src := NewDevTo(config.CrawlerConfig{FetchBody: boolPtr(true)})
	arts, _, empty, err := src.FetchSince(context.Background(), 1, 10, time.Time{})
	if err != nil || empty || len(arts) != 1 {
		t.Fatalf("fetch failed: err=%v empty=%v n=%d", err, empty, len(arts))
	}
	if arts[0].ContentMD != "" {
		t.Fatalf("404 body must leave ContentMD empty, got %q", arts[0].ContentMD)
	}
}
