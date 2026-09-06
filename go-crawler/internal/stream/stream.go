package stream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"en-tech-pipeline/go-crawler/internal/config"
	"en-tech-pipeline/go-crawler/internal/source"
)

// Producer publishes ArticleRaw records into a Redis Stream with atomic dedup.
// A Lua script checks a SET membership, adds the hash, and XADDs only on miss.
type Producer struct {
	rdb    *redis.Client
	stream string
	group  string
	maxLen int
}

// dedupScript atomically checks SET membership, records the hash, and XADDs
// only on a miss. When maxlen > 0 the stream is trimmed with XADD MAXLEN ~
// so Redis memory stays bounded (dedup state in the SET is unaffected).
var dedupScript = redis.NewScript(`
local key = KEYS[1]
local hash = ARGV[1]
local stream = ARGV[2]
local field = ARGV[3]
local val = ARGV[4]
local maxlen = tonumber(ARGV[5])
if redis.call('SISMEMBER', key, hash) == 1 then
  return 0
end
redis.call('SADD', key, hash)
if maxlen > 0 then
  redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*', field, val)
else
  redis.call('XADD', stream, '*', field, val)
end
return 1
`)

func HashURL(url string) string {
	sum := sha256.Sum256([]byte(url))
	return hex.EncodeToString(sum[:])
}

func NewProducer(cfg config.RedisConfig) (*Producer, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	p := &Producer{rdb: rdb, stream: cfg.Stream, group: cfg.ConsumerGroup, maxLen: cfg.StreamMaxLen}
	// ensure consumer group exists (idempotent)
	_, _ = rdb.XGroupCreateMkStream(ctx, p.stream, p.group, "0").Result()
	return p, nil
}

// Publish atomically dedups by url_hash and enqueues JSON payload to the stream.
// Returns published=true if the item was new.
func (p *Producer) Publish(ctx context.Context, a source.ArticleRaw) (bool, error) {
	hash := HashURL(a.URL)
	payload, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	res, err := dedupScript.Run(ctx, p.rdb,
		[]string{"seen:" + p.stream},
		hash, p.stream, "article", payload, p.maxLen,
	).Int()
	if err != nil {
		return false, err
	}
	if res == 1 {
		if err := p.rdb.Incr(ctx, "metrics:"+p.stream+":produced").Err(); err != nil {
			log.Printf("[stream] produced metric incr failed: %v", err)
		}
		if a.SourceType != "" {
			if err := p.rdb.HIncrBy(ctx, "metrics:articles:produced_by_source", a.SourceType, 1).Err(); err != nil {
				log.Printf("[stream] produced_by_source metric incr failed: %v", err)
			}
		}
	}
	return res == 1, nil
}

// RunStats is one scheduler run's summary, recorded to Redis so the monitor
// sampler and the dashboard can show per-source heartbeat without parsing logs.
type RunStats struct {
	Source        string `json:"source"`
	FinishedAt    string `json:"finished_at"`
	Pages         int    `json:"pages"`
	Published     int    `json:"published"`
	FailedPages   int    `json:"failed_pages"`
	PublishErrors int    `json:"publish_errors"`
	DurationMs    int64  `json:"duration_ms"`
}

// RecordRun stores the latest run summary per source (pipeline:runs hash).
// Best-effort: monitoring must never fail a crawl.
func (p *Producer) RecordRun(ctx context.Context, s RunStats) {
	if s.Source == "" {
		return
	}
	s.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	payload, err := json.Marshal(s)
	if err != nil {
		log.Printf("[stream] record run marshal failed: %v", err)
		return
	}
	rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.rdb.HSet(rctx, "pipeline:runs", s.Source, payload).Err(); err != nil {
		log.Printf("[stream] record run failed: %v", err)
	}
}

func (p *Producer) Close() error {
	return p.rdb.Close()
}
