package source

import (
	"context"
	"encoding/json"
	"os"
	"time"
)

// DefaultUserAgent identifies the bot to upstream APIs. Before long-running
// deployments, set crawler.user_agent (yaml) or CRAWLER_USER_AGENT (env) to a
// real contact address — upstreams throttle or block unidentified bots.
const DefaultUserAgent = "WebSpider/0.2 (+https://example.com)"

func resolveUserAgent(cfgUA string) string {
	if ua := os.Getenv("CRAWLER_USER_AGENT"); ua != "" {
		return ua
	}
	if cfgUA != "" {
		return cfgUA
	}
	return DefaultUserAgent
}

// ArticleRaw is the normalized record produced by any source before it enters
// the Redis Stream. Payload keeps the original API JSON for the processor.
// ContentMD is the optional inline body: API-first sources (Stack Exchange)
// deliver clean markdown directly, and the processor then skips the HTML
// fetch/extract stages entirely.
type ArticleRaw struct {
	URL        string          `json:"url"`
	Title      string          `json:"title"`
	Author     string          `json:"author"`
	Published  time.Time       `json:"published_at"`
	Tags       []string        `json:"tags"`
	SourceType string          `json:"source_type"`
	Payload    json.RawMessage `json:"payload"`
	ContentMD  string          `json:"content_md,omitempty"`
}

// Source fetches pages of articles. FetchSince returns articles published at or
// after `since` (zero time = no filter), the page's oldest published time, and
// whether the page was empty (used by the scheduler to stop).
type Source interface {
	Name() string
	FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error)
}
