package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const devToBase = "https://dev.to/api/articles"

type DevTo struct {
	client *http.Client
}

func NewDevTo() *DevTo {
	return &DevTo{client: &http.Client{Timeout: 15 * time.Second}}
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

// FetchSince fetches one page; returns articles at/after `since` (zero = no
// filter), the oldest published time seen, and whether the page was empty for
// the cursor (all items older than `since`).
func (d *DevTo) FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, devToBase, nil)
	if err != nil {
		return nil, time.Time{}, false, err
	}
	q := req.URL.Query()
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("per_page", fmt.Sprintf("%d", perPage))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WebSpider/0.1 (+https://example.com)")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, time.Time{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, false, fmt.Errorf("devto status %d", resp.StatusCode)
	}

	var items []devToArticle
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, time.Time{}, false, err
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
