package stream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
}

var dedupScript = redis.NewScript(`
local key = KEYS[1]
local hash = ARGV[1]
local stream = ARGV[2]
local field = ARGV[3]
local val = ARGV[4]
if redis.call('SISMEMBER', key, hash) == 1 then
  return 0
end
redis.call('SADD', key, hash)
redis.call('XADD', stream, '*', field, val)
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
	p := &Producer{rdb: rdb, stream: cfg.Stream, group: cfg.ConsumerGroup}
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
		hash, p.stream, "article", payload,
	).Int()
	if err != nil {
		return false, err
	}
	if res == 1 {
		p.rdb.Incr(ctx, "metrics:"+p.stream+":produced")
	}
	return res == 1, nil
}

func (p *Producer) Close() error {
	return p.rdb.Close()
}
