package source

import (
	"context"
	"encoding/json"
	"time"
)

// ArticleRaw is the normalized record produced by any source before it enters
// the Redis Stream. Payload keeps the original API JSON for the processor.
type ArticleRaw struct {
	URL        string          `json:"url"`
	Title      string          `json:"title"`
	Author     string          `json:"author"`
	Published  time.Time       `json:"published_at"`
	Tags       []string        `json:"tags"`
	SourceType string          `json:"source_type"`
	Payload    json.RawMessage `json:"payload"`
}

// Source fetches pages of articles. FetchSince returns articles published at or
// after `since` (zero time = no filter), the page's oldest published time, and
// whether the page was empty (used by the scheduler to stop).
type Source interface {
	Name() string
	FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error)
}
