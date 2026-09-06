package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"en-tech-pipeline/go-crawler/internal/config"
)

// /api/articles (default) is popularity-ordered — its newest item can lag by
// days, which starves the incremental since-hours window. /latest is strictly
// newest-first, which the scheduler's empty-page/cursor stop logic requires.
const devToBase = "https://dev.to/api/articles/latest"

type DevTo struct {
	client    *http.Client
	userAgent string
}

func NewDevTo(cfg config.CrawlerConfig) *DevTo {
	return &DevTo{
		client:    &http.Client{Timeout: 15 * time.Second},
		userAgent: resolveUserAgent(cfg.UserAgent),
	}
}

func (d *DevTo) Name() string { return "devto" }

// devToArticle mirrors the DEV.to article API. TagList is unstable: it can be a
// JSON string array OR an array of objects, so we keep it raw and parse later.
type devToArticle struct {
	URL         string          `json:"url"`
	Title       string          `json:"title"`
	PublishedAt time.Time       `json:"published_at"`
	TagList     json.RawMessage `json:"tag_list"`
	User        struct {
		Name string `json:"name"`
	} `json:"user"`
}

func parseTags(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var strs []string
	if err := json.Unmarshal(raw, &strs); err == nil {
		return strs
	}
	// tags may be [{name:"go"}, ...]
	var objs []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &objs); err == nil {
		out := make([]string, 0, len(objs))
		for _, o := range objs {
			if o.Name != "" {
				out = append(out, o.Name)
			}
		}
		return out
	}
	return nil
}

// parseRetryAfter converts a Retry-After header (delta-seconds or HTTP-date)
// into a duration. Returns 0 when absent, unparseable, or non-positive —
// callers treat <=0 as "no explicit wait".
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func backoffSeconds(attempt int) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * time.Second
}

// sleepBackoff sleeps for d (default 500ms), aborting early if ctx is done.
func sleepBackoff(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = 500 * time.Millisecond
	}
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// FetchSince fetches one page; returns articles at/after `since` (zero = no
// filter), the oldest published time seen, and whether the page was empty for
// the cursor (all items older than `since`).
const maxDevToRetries = 3

func (d *DevTo) FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error) {
	var (
		lastErr error
		items   []devToArticle
	)
	for attempt := 0; attempt < maxDevToRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, devToBase, nil)
		if err != nil {
			return nil, time.Time{}, false, err
		}
		q := req.URL.Query()
		q.Set("page", fmt.Sprintf("%d", page))
		q.Set("per_page", fmt.Sprintf("%d", perPage))
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", d.userAgent)

		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = err
			if !sleepBackoff(ctx, backoffSeconds(attempt)) {
				return nil, time.Time{}, false, ctx.Err()
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			decodeErr := json.NewDecoder(resp.Body).Decode(&items)
			resp.Body.Close()
			if decodeErr != nil {
				return nil, time.Time{}, false, decodeErr
			}
			break
		}
		// 429: honour Retry-After when present, otherwise back off on the
		// seconds ladder. dev.to sends no rate-limit headers at all, so the
		// 500ms default sleep would land inside the throttle window and burn
		// every attempt. 5xx: exponential backoff.
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		resp.Body.Close()
		lastErr = fmt.Errorf("devto status %d", resp.StatusCode)
		if retryAfter <= 0 {
			retryAfter = backoffSeconds(attempt)
		}
		if !sleepBackoff(ctx, retryAfter) {
			return nil, time.Time{}, false, ctx.Err()
		}
	}
	if lastErr != nil {
		return nil, time.Time{}, false, fmt.Errorf("devto after %d attempts: %w", maxDevToRetries, lastErr)
	}

	if len(items) == 0 {
		return nil, time.Time{}, true, nil
	}

	out := make([]ArticleRaw, 0, len(items))
	oldest := time.Time{}
	for _, it := range items {
		if !since.IsZero() && it.PublishedAt.Before(since) {
			// page is sorted newest-first; older items beyond window are skipped
			continue
		}
		payload, _ := json.Marshal(it)
		out = append(out, ArticleRaw{
			URL:        it.URL,
			Title:      it.Title,
			Author:     it.User.Name,
			Published:  it.PublishedAt,
			Tags:       parseTags(it.TagList),
			SourceType: "devto",
			Payload:    payload,
		})
		if oldest.IsZero() || it.PublishedAt.Before(oldest) {
			oldest = it.PublishedAt
		}
	}
	empty := len(out) == 0
	return out, oldest, empty, nil
}
