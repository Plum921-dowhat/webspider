package scheduler

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"en-tech-pipeline/go-crawler/internal/source"
	"en-tech-pipeline/go-crawler/internal/stream"
)

type pageResult struct {
	page  int
	empty bool
}

// Scheduler drives paginated fetching with a worker pool + rate limiter.
// It stops when a page comes back empty or older than the since cursor, or when
// maxPages is reached, or when ctx is cancelled.
type Scheduler struct {
	src      source.Source
	prod     *stream.Producer
	perPage  int
	maxPages int
	since    time.Time
	limiter  *rate.Limiter
}

func New(src source.Source, prod *stream.Producer, perPage, maxPages int, since time.Time, ratePerSec int) *Scheduler {
	return &Scheduler{
		src:      src,
		prod:     prod,
		perPage:  perPage,
		maxPages: maxPages,
		since:    since,
		limiter:  rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	sem := make(chan struct{}, 8) // bounded concurrency
	results := make(chan pageResult, 8)
	var wg sync.WaitGroup

	nextPage := 1
	pending := 0
	stop := false

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
				// best-effort: mark page as processed so loop continues
				results <- pageResult{page: page, empty: false}
				return
			}
			for _, a := range articles {
				if _, perr := s.prod.Publish(ctx, a); perr != nil {
					// log via stderr; keep pipeline moving
					continue
				}
			}
			results <- pageResult{page: page, empty: empty}
		}(page)

		// drain one completed page to detect termination promptly
		if pending > 0 {
			select {
			case r := <-results:
				pending--
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
		_ = r
	}
	return nil
}
