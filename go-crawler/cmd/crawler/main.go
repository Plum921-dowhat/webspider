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
	"devto":         func(cfg config.CrawlerConfig) source.Source { return source.NewDevTo() },
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

	s := scheduler.New(src, prod, cfg.Crawler.PerPage, cfg.Crawler.MaxPages, cfg.Crawler.MaxConcurrency,
		cfg.Since(), cfg.Crawler.RatePerSec, cfg.Crawler.MaxPageFailures)

	interval := time.Duration(cfg.Crawler.IntervalMinutes) * time.Minute
	if interval <= 0 {
		runOnce(ctx, s)
		return
	}

	log.Printf("crawler daemon started: interval=%s since_hours=%d", interval, cfg.Crawler.SinceHours)
	for {
		runOnce(ctx, s)
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

func registryKeys() []string {
	keys := make([]string, 0, len(sourceRegistry))
	for k := range sourceRegistry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func runOnce(ctx context.Context, s *scheduler.Scheduler) {
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
