package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"en-tech-pipeline/go-crawler/internal/source"
	"en-tech-pipeline/go-crawler/internal/stream"
)

type pageResult struct {
	page      int
	empty     bool
	failed    bool
	published int
	pubErrs   int
}

// Publisher is the subset of stream.Producer the scheduler depends on; an
// interface so tests can inject a fake without Redis.
type Publisher interface {
	Publish(ctx context.Context, a source.ArticleRaw) (bool, error)
	RecordRun(ctx context.Context, s stream.RunStats)
}

// Scheduler drives paginated fetching with a worker pool + rate limiter.
// It stops when a page comes back empty or older than the since cursor, or when
// maxPages is reached, or when too many pages failed (circuit breaker), or when
// ctx is cancelled.
type Scheduler struct {
	src             source.Source
	prod            Publisher
	perPage         int
	maxPages        int
	concurrency     int
	since           time.Time
	maxPageFailures int
	limiter         *rate.Limiter
}

func New(src source.Source, prod Publisher, perPage, maxPages, concurrency int, since time.Time, ratePerSec, maxPageFailures int) *Scheduler {
	if concurrency <= 0 {
		concurrency = 8
	}
	if maxPageFailures <= 0 {
		maxPageFailures = 3
	}
	return &Scheduler{
		src:             src,
		prod:            prod,
		perPage:         perPage,
		maxPages:        maxPages,
		concurrency:     concurrency,
		since:           since,
		maxPageFailures: maxPageFailures,
		limiter:         rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	start := time.Now()
	sem := make(chan struct{}, s.concurrency) // bounded concurrency
	results := make(chan pageResult, s.concurrency)
	var wg sync.WaitGroup

	// run-level stats, aggregated in the drain paths below (single consumer,
	// so plain counters are safe)
	nextPage := 1
	pending := 0
	stop := false
	var pages, published, pubErrs, failedPages int

	absorb := func(r pageResult) {
		pages++
		published += r.published
		pubErrs += r.pubErrs
		if r.failed {
			failedPages++
			if failedPages >= s.maxPageFailures {
				log.Printf("[scheduler] circuit breaker: %d pages failed this run, stopping", failedPages)
				stop = true
			}
		}
	}

	for !stop {
		// respect rate limit before scheduling next page
		if err := s.limiter.Wait(ctx); err != nil {
			return err // ctx cancelled
		}
		if s.maxPages > 0 && nextPage > s.maxPages {
			stop = true
			break
		}

		page := nextPage
		nextPage++
		pending++
		wg.Add(1)
		sem <- struct{}{}

		go func(page int) {
			defer wg.Done()
			defer func() { <-sem }()

			articles, _, empty, err := s.src.FetchSince(ctx, page, s.perPage, s.since)
			if err != nil {
				log.Printf("[scheduler] page %d fetch failed after retries: %v", page, err)
				results <- pageResult{page: page, failed: true}
				return
			}
			res := pageResult{page: page, empty: empty}
			for _, a := range articles {
				ok, perr := s.prod.Publish(ctx, a)
				if perr != nil {
					res.pubErrs++
					continue
				}
				if ok {
					res.published++
				}
			}
			results <- res
		}(page)

		// drain one completed page to detect termination promptly
		if pending > 0 {
			select {
			case r := <-results:
				pending--
				absorb(r)
				if r.empty {
					stop = true
				}
			case <-ctx.Done():
				stop = true
			default:
				// allow speculative prefetch of a couple pages
			}
		}
	}

	// wait for in-flight pages
	go func() {
		wg.Wait()
		close(results)
	}()
	for r := range results {
		absorb(r)
	}
	log.Printf("[scheduler] run summary: pages=%d published=%d publish_errors=%d failed_pages=%d",
		pages, published, pubErrs, failedPages)
	// best-effort heartbeat for the monitor/dashboard; survives ctx cancel
	s.prod.RecordRun(context.Background(), stream.RunStats{
		Source:        s.src.Name(),
		Pages:         pages,
		Published:     published,
		FailedPages:   failedPages,
		PublishErrors: pubErrs,
		DurationMs:    time.Since(start).Milliseconds(),
	})
	return nil
}
