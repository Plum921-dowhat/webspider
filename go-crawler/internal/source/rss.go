package source

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	gofeed "github.com/mmcdole/gofeed"

	"en-tech-pipeline/go-crawler/internal/config"
)

// RSS is a generic Atom/RSS/JSON-feed adapter driven by a configured feed
// list (crawler.rss_feeds). One run = one poll: every feed is fetched once
// (page 1); page > 1 is always empty, which stops the scheduler immediately.
// Articles are the feed items' links — full HTML pages fetched by the
// processor through the normal trafilatura path, so per-domain rate limits
// apply. Feeds that fail to fetch are skipped for the run (logged), never
// fail the whole page.
type RSS struct {
	feeds  []string
	client *http.Client
}

func NewRSS(cfg config.CrawlerConfig) *RSS {
	return &RSS{
		feeds:  cfg.RSSFeeds,
		client: &http.Client{Timeout: 20 * time.Second},
	}
}

func (r *RSS) Name() string { return "rss" }

func (r *RSS) FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error) {
	if page > 1 {
		return nil, time.Time{}, true, nil
	}
	if len(r.feeds) == 0 {
		return nil, time.Time{}, true, nil
	}

	parser := gofeed.NewParser()
	parser.Client = r.client

	oldest := time.Time{}
	out := make([]ArticleRaw, 0, perPage)
	for _, feedURL := range r.feeds {
		feed, err := parser.ParseURLWithContext(feedURL, ctx)
		if err != nil {
			log.Printf("[rss] feed fetch failed, skipping: %s: %v", feedURL, err)
			continue
		}
		for _, item := range feed.Items {
			if item.Link == "" {
				continue
			}
			published := itemTime(item)
			if !since.IsZero() && published.Before(since) {
				continue
			}
			if len(out) >= perPage {
				break
			}
			payload, _ := json.Marshal(map[string]any{
				"feed":    feedURL,
				"guid":    item.GUID,
				"summary": truncate(item.Description, 500),
			})
			out = append(out, ArticleRaw{
				URL:        item.Link,
				Title:      item.Title,
				Author:     itemAuthor(item),
				Published:  published,
				SourceType: "rss",
				Payload:    payload,
			})
			if oldest.IsZero() || published.Before(oldest) {
				oldest = published
			}
		}
	}
	return out, oldest, len(out) == 0, nil
}

func itemTime(item *gofeed.Item) time.Time {
	if item.PublishedParsed != nil {
		return item.PublishedParsed.UTC()
	}
	if item.UpdatedParsed != nil {
		return item.UpdatedParsed.UTC()
	}
	return time.Time{}
}

func itemAuthor(item *gofeed.Item) string {
	if item.Author != nil && item.Author.Name != "" {
		return item.Author.Name
	}
	if len(item.Authors) > 0 && item.Authors[0] != nil {
		return item.Authors[0].Name
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
