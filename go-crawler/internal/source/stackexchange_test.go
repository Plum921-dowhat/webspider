package source

import (
	"net/url"
	"strings"
	"testing"

	"en-tech-pipeline/go-crawler/internal/config"
)

func TestBestAnswerPrefersAccepted(t *testing.T) {
	answers := []seAnswer{
		{AnswerID: 1, Score: 10},
		{AnswerID: 2, Score: 2, Accepted: true},
	}
	got := bestAnswer(answers)
	if got.AnswerID != 2 {
		t.Fatalf("accepted answer should win, got id %d", got.AnswerID)
	}
}

func TestBestAnswerHighestPositiveWhenNoAccepted(t *testing.T) {
	answers := []seAnswer{
		{AnswerID: 1, Score: 5},
		{AnswerID: 2, Score: 9},
		{AnswerID: 3, Score: -1},
	}
	got := bestAnswer(answers)
	if got.AnswerID != 2 {
		t.Fatalf("highest positive should win, got id %d", got.AnswerID)
	}
}

func TestBestAnswerNonePositive(t *testing.T) {
	answers := []seAnswer{{AnswerID: 1, Score: -3}}
	if got := bestAnswer(answers); got != nil {
		t.Fatalf("want nil for all-negative answers, got %+v", got)
	}
}

func TestStackExchangeComposeDoc(t *testing.T) {
	s := NewStackExchange(config.CrawlerConfig{})
	q := seQuestion{
		Title: "How to &quot;retry&quot; HTTP calls",
		Body:  "<p>Use <code>backoff</code> with <a href=\"https://pkg.go.dev\">jitter</a>.</p>",
	}
	answers := []seAnswer{{Body: "<p>Try <strong>exponential</strong> backoff.</p>", Score: 3}}

	md, ok := s.composeDoc(q, answers)
	if !ok {
		t.Fatalf("doc should compose")
	}
	for _, want := range []string{"# How to \"retry\" HTTP calls", "## Top answer", "backoff", "exponential"} {
		if !strings.Contains(md, want) {
			t.Fatalf("composed doc missing %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "&quot;") {
		t.Fatalf("title entities must be unescaped:\n%s", md)
	}
}

func TestStackExchangeComposeDocRequiresAnswer(t *testing.T) {
	s := NewStackExchange(config.CrawlerConfig{SERequireAnswered: true})
	q := seQuestion{Title: "t", Body: "<p>body</p>"}
	if _, ok := s.composeDoc(q, nil); ok {
		t.Fatalf("question without answers must be skipped when requireAnswered")
	}
}

func TestStackExchangeBuildURL(t *testing.T) {
	s := NewStackExchange(config.CrawlerConfig{SESite: "stackoverflow", SEAPIKey: "k123"})
	params := url.Values{"site": {"stackoverflow"}, "sort": {"creation"}}
	got := s.buildURL("/questions", params)
	if !strings.Contains(got, "site=stackoverflow") || !strings.Contains(got, "key=k123") {
		t.Fatalf("unexpected url: %s", got)
	}
}

func TestStackExchangeBuildURLNoKeyOverwrite(t *testing.T) {
	s := NewStackExchange(config.CrawlerConfig{SEAPIKey: "k123"})
	params := url.Values{"key": {"explicit"}}
	got := s.buildURL("/questions", params)
	if !strings.Contains(got, "key=explicit") {
		t.Fatalf("explicit key param must win: %s", got)
	}
}
