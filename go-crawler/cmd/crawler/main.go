package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"en-tech-pipeline/go-crawler/internal/config"
	"en-tech-pipeline/go-crawler/internal/scheduler"
	"en-tech-pipeline/go-crawler/internal/source"
	"en-tech-pipeline/go-crawler/internal/stream"
)

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

	src := source.NewDevTo()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := scheduler.New(src, prod, cfg.Crawler.PerPage, cfg.Crawler.MaxPages, cfg.Crawler.MaxConcurrency, cfg.Since(), cfg.Crawler.RatePerSec)

	start := time.Now()
	if err := s.Run(ctx); err != nil {
		if ctx.Err() != nil {
			log.Println("interrupted, shutting down")
		} else {
			log.Fatalf("scheduler: %v", err)
		}
	}
	fmt.Printf("crawl finished in %s\n", time.Since(start))
}
