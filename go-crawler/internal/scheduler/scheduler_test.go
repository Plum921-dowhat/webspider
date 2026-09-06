package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"en-tech-pipeline/go-crawler/internal/source"
	"en-tech-pipeline/go-crawler/internal/stream"
)

type pageSpec struct {
	articles int
	empty    bool
	err      error
}

type fakeSource struct {
	name  string
	pages map[int]pageSpec
	mu    sync.Mutex
}

func (f *fakeSource) Name() string {
	if f.name == "" {
		return "fake"
	}
	return f.name
}

func (f *fakeSource) FetchSince(ctx context.Context, page, perPage int, since time.Time) ([]source.ArticleRaw, time.Time, bool, error) {
	f.mu.Lock()
	spec, ok := f.pages[page]
	f.mu.Unlock()
	if !ok {
		spec = pageSpec{empty: true}
	}
	if spec.err != nil {
		return nil, time.Time{}, false, spec.err
	}
	out := make([]source.ArticleRaw, spec.articles)
	for i := range out {
		out[i] = source.ArticleRaw{URL: "https://example.com/p", SourceType: f.Name()}
	}
	return out, time.Time{}, spec.empty, nil
}

type fakePublisher struct {
	mu        sync.Mutex
	published int
	pubErrs   int
	failAll   bool
	runs      []stream.RunStats
}

func (f *fakePublisher) Publish(ctx context.Context, a source.ArticleRaw) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAll {
		f.pubErrs++
		return false, errors.New("redis down")
	}
	f.published++
	return true, nil
}

func (f *fakePublisher) RecordRun(ctx context.Context, s stream.RunStats) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = append(f.runs, s)
}

func newTestScheduler(src source.Source, pub *fakePublisher, maxPageFailures int) *Scheduler {
	return New(src, pub, 10, 0, 1, time.Time{}, 1000, maxPageFailures)
}

func TestRunStopsOnEmptyPageAndRecordsHeartbeat(t *testing.T) {
	src := &fakeSource{pages: map[int]pageSpec{
		1: {articles: 2},
		2: {empty: true},
	}}
	pub := &fakePublisher{}
	err := newTestScheduler(src, pub, 3).Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if pub.published != 2 {
		t.Fatalf("want 2 published, got %d", pub.published)
	}
	if len(pub.runs) != 1 {
		t.Fatalf("want exactly one recorded run, got %d", len(pub.runs))
	}
	run := pub.runs[0]
	if run.Source != "fake" || run.Published != 2 || run.FailedPages != 0 {
		t.Fatalf("unexpected run stats: %+v", run)
	}
	// FinishedAt is stamped by the real Producer.RecordRun, not the scheduler.
}

func TestRunCircuitBreakerStopsAfterRepeatedFailures(t *testing.T) {
	src := &fakeSource{pages: map[int]pageSpec{
		1: {articles: 1},
		2: {err: errors.New("429")},
		3: {err: errors.New("429")},
		4: {err: errors.New("429")},
		5: {err: errors.New("429")},
	}}
	pub := &fakePublisher{}
	if err := newTestScheduler(src, pub, 2).Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if len(pub.runs) != 1 || pub.runs[0].FailedPages < 2 {
		t.Fatalf("breaker should stop after >=2 failed pages, got %+v", pub.runs)
	}
	// healthy first page must have been published before the breaker fired
	if pub.runs[0].Published != 1 {
		t.Fatalf("want 1 published before breaker, got %d", pub.runs[0].Published)
	}
}

func TestRunCountsPublishErrors(t *testing.T) {
	src := &fakeSource{pages: map[int]pageSpec{
		1: {articles: 3},
		2: {empty: true},
	}}
	pub := &fakePublisher{failAll: true}
	if err := newTestScheduler(src, pub, 3).Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	run := pub.runs[0]
	if run.Published != 0 || run.PublishErrors < 3 {
		t.Fatalf("want 0 published and >=3 publish errors, got %+v", run)
	}
}
