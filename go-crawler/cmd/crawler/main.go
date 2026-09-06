package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"en-tech-pipeline/go-crawler/internal/config"
	"en-tech-pipeline/go-crawler/internal/scheduler"
	"en-tech-pipeline/go-crawler/internal/source"
	"en-tech-pipeline/go-crawler/internal/stream"
)

// sourceRegistry maps configured source names to constructors. Add a new
// Source implementation here to make it selectable via config.yaml. The
// constructor receives the crawler config so sources can carry their own
// options (e.g. rss_feeds, se_site).
var sourceRegistry = map[string]func(config.CrawlerConfig) source.Source{
	"devto":         func(cfg config.CrawlerConfig) source.Source { return source.NewDevTo(cfg) },
	"stackexchange": func(cfg config.CrawlerConfig) source.Source { return source.NewStackExchange(cfg) },
	"rss":           func(cfg config.CrawlerConfig) source.Source { return source.NewRSS(cfg) },
}

func main() {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	prod, err := stream.NewProducer(cfg.Redis)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer prod.Close()

	newSource, ok := sourceRegistry[cfg.Crawler.Source]
	if !ok {
		log.Fatalf("unknown source %q (available: %v)", cfg.Crawler.Source, registryKeys())
	}
	src := newSource(cfg.Crawler)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := buildScheduler(cfg, prod, src)

	interval := time.Duration(cfg.Crawler.IntervalMinutes) * time.Minute
	if interval <= 0 {
		runOnce(ctx, cfg, prod, s)
		return
	}

	log.Printf("crawler daemon started: interval=%s since_hours=%d", interval, cfg.Crawler.SinceHours)
	for {
		// rebuild the scheduler every cycle: with use_cursor the window
		// start advances after each successful run
		s = buildScheduler(cfg, prod, src)
		runOnce(ctx, cfg, prod, s)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			log.Println("interrupted, shutting down")
			return
		case <-time.After(interval):
		}
	}
}

// resolveSince picks the incremental window start for a run: the per-source
// Redis cursor (newest seen published_at, minus a small overlap) when
// available, else the config's since-hours window. The cursor covers sleep
// gaps longer than the fixed window; the overlap guards boundary articles.
func resolveSince(base, cursor time.Time, hasCursor bool, overlap time.Duration) time.Time {
	if !hasCursor || cursor.IsZero() {
		return base
	}
	withOverlap := cursor.Add(-overlap)
	if base.IsZero() || withOverlap.After(base) {
		return withOverlap
	}
	return base
}

func buildScheduler(cfg *config.Config, prod *stream.Producer, src source.Source) *scheduler.Scheduler {
	base := cfg.Since()
	since := base
	if cfg.Crawler.UseCursor {
		if cursor, ok := prod.GetCursor(context.Background(), src.Name()); ok {
			since = resolveSince(base, cursor, true, 5*time.Minute)
		}
	}
	return scheduler.New(src, prod, cfg.Crawler.PerPage, cfg.Crawler.MaxPages, cfg.Crawler.MaxConcurrency,
		since, cfg.Crawler.RatePerSec, cfg.Crawler.MaxPageFailures)
}

func registryKeys() []string {
	keys := make([]string, 0, len(sourceRegistry))
	for k := range sourceRegistry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func runOnce(ctx context.Context, cfg *config.Config, prod *stream.Producer, s *scheduler.Scheduler) {
	start := time.Now()
	if err := s.Run(ctx); err != nil {
		if ctx.Err() != nil {
			log.Println("interrupted, shutting down")
		} else {
			log.Printf("scheduler run failed: %v", err)
		}
		return
	}
	log.Printf("crawl finished in %s", time.Since(start))
}
