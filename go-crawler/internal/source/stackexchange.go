package source

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JohannesKaufmann/html-to-markdown/v2/converter"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/base"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/commonmark"

	"en-tech-pipeline/go-crawler/internal/config"
)

const seBase = "https://api.stackexchange.com/2.3"

// StackExchange delivers Q&A documents with inline markdown (ContentMD), so
// the processor never fetches SO pages. Polls use sort=activity with a since
// window ("recently answered"), and the listing call uses the default filter
// so triage on is_answered/score is free; only keepers' bodies and answers
// are fetched (one batched call each).
//
// Editorial filters: questions below se_min_score are dropped, and with
// se_require_answered only answered questions are published — fresh SO
// questions are otherwise dominated by low-quality, unanswered posts.
type StackExchange struct {
	site            string
	minScore        int
	requireAnswered bool
	apiKey          string
	client          *http.Client
	conv            *converter.Converter
}

func NewStackExchange(cfg config.CrawlerConfig) *StackExchange {
	site := cfg.SESite
	if site == "" {
		site = "stackoverflow"
	}
	apiKey := os.Getenv("SE_API_KEY")
	if apiKey == "" {
		apiKey = cfg.SEAPIKey
	}
	return &StackExchange{
		site:            site,
		minScore:        cfg.SEMinScore,
		requireAnswered: cfg.SERequireAnswered,
		apiKey:          apiKey,
		client:          &http.Client{Timeout: 20 * time.Second},
		conv: converter.NewConverter(
			converter.WithPlugins(
				base.NewBasePlugin(),
				commonmark.NewCommonmarkPlugin(),
			),
		),
	}
}

func (s *StackExchange) Name() string { return "stackexchange" }

type seQuestion struct {
	QuestionID   int      `json:"question_id"`
	Title        string   `json:"title"`
	Body         string   `json:"body"`
	Link         string   `json:"link"`
	Score        int      `json:"score"`
	CreationDate int64    `json:"creation_date"`
	IsAnswered   bool     `json:"is_answered"`
	Tags         []string `json:"tags"`
	Owner        struct {
		DisplayName string `json:"display_name"`
	} `json:"owner"`
}

type seAnswer struct {
	AnswerID   int    `json:"answer_id"`
	QuestionID int    `json:"question_id"`
	Body       string `json:"body"`
	Score      int    `json:"score"`
	Accepted   bool   `json:"is_accepted"`
}

type seEnvelope[T any] struct {
	Items          []T    `json:"items"`
	HasMore        bool   `json:"has_more"`
	QuotaRemaining int    `json:"quota_remaining"`
	ErrorID        int    `json:"error_id"`
	ErrorName      string `json:"error_name"`
	ErrorMessage   string `json:"error_message"`
	Backoff        int    `json:"backoff"`
}

const maxSERetries = 3

// getJSON fetches one SE API URL with retries. SE signals throttling with a
// 200 + error_id 502 body (sometimes a `backoff` seconds hint), so we retry on
// those in addition to transport/5xx failures.
func (s *StackExchange) getJSON(ctx context.Context, path string, out any) error {
	var lastErr error
	for attempt := 0; attempt < maxSERetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "en-tech-corpus-bot/1.0 (+https://example.com/bot)")
		// NOTE: no manual Accept-Encoding — SE gzip-encodes responses, and
		// Go's transport only auto-decompresses when IT added the header.

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			if !sleepBackoff(ctx, backoffSeconds(attempt)) {
				return ctx.Err()
			}
			continue
		}
		var env seEnvelope[json.RawMessage]
		dec := json.NewDecoder(resp.Body)
		decErr := dec.Decode(&env)
		resp.Body.Close()
		if decErr != nil {
			lastErr = fmt.Errorf("decode %s: %w (status %d)", path, decErr, resp.StatusCode)
			if !sleepBackoff(ctx, backoffSeconds(attempt)) {
				return ctx.Err()
			}
			continue
		}
		if env.ErrorID != 0 {
			wait := time.Duration(max(env.Backoff, 5)) * time.Second
			lastErr = fmt.Errorf("se api error %d (%s): %s", env.ErrorID, env.ErrorName, env.ErrorMessage)
			if !sleepBackoff(ctx, wait) {
				return ctx.Err()
			}
			continue
		}
		// re-decode items into the caller's type
		raw, err := json.Marshal(env.Items)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, out)
	}
	return fmt.Errorf("se after %d attempts: %w", maxSERetries, lastErr)
}

func (s *StackExchange) buildURL(path string, params url.Values) string {
	if s.apiKey != "" && params.Get("key") == "" {
		params.Set("key", s.apiKey)
	}
	return fmt.Sprintf("%s%s?%s", seBase, path, params.Encode())
}

// bestAnswer picks the accepted answer, else the highest-scored positive one.
func bestAnswer(answers []seAnswer) *seAnswer {
	var best *seAnswer
	for i := range answers {
		a := &answers[i]
		if a.Accepted {
			return a
		}
		if a.Score <= 0 {
			continue
		}
		if best == nil || a.Score > best.Score {
			best = a
		}
	}
	return best
}

func (s *StackExchange) composeDoc(q seQuestion, answers []seAnswer) (string, bool) {
	if s.requireAnswered {
		if bestAnswer(answers) == nil {
			return "", false
		}
	}
	qMd, err := s.conv.ConvertString(q.Body)
	if err != nil || strings.TrimSpace(qMd) == "" {
		return "", false
	}
	var b strings.Builder
	b.WriteString("# " + html.UnescapeString(q.Title) + "\n\n")
	b.WriteString(strings.TrimSpace(qMd) + "\n")
	if a := bestAnswer(answers); a != nil {
		aMd, err := s.conv.ConvertString(a.Body)
		if err == nil && strings.TrimSpace(aMd) != "" {
			b.WriteString("\n## Top answer\n\n" + strings.TrimSpace(aMd) + "\n")
		}
	}
	return b.String(), true
}

func (s *StackExchange) FetchSince(ctx context.Context, page int, perPage int, since time.Time) ([]ArticleRaw, time.Time, bool, error) {
	// sort=activity + min=since: questions whose latest activity (new answer,
	// edit) falls in the window — "recently answered", regardless of ask date.
	// The listing uses the DEFAULT filter (no bodies) so client-side triage
	// (is_answered, score) costs no quota; bodies/answers are fetched in one
	// batched call each for keepers only.
	params := url.Values{
		"order":    {"desc"},
		"sort":     {"activity"},
		"site":     {s.site},
		"pagesize": {strconv.Itoa(min(perPage, 100))},
		"page":     {strconv.Itoa(page)},
	}
	if !since.IsZero() {
		params.Set("min", strconv.FormatInt(since.Unix(), 10))
	}

	var questions []seQuestion
	if err := s.getJSON(ctx, s.buildURL("/questions", params), &questions); err != nil {
		return nil, time.Time{}, false, err
	}
	if len(questions) == 0 {
		return nil, time.Time{}, true, nil
	}

	keepers := make([]seQuestion, 0, len(questions))
	oldest := time.Time{}
	for _, q := range questions {
		published := time.Unix(q.CreationDate, 0).UTC()
		if oldest.IsZero() || published.Before(oldest) {
			oldest = published
		}
		if q.Score < s.minScore {
			continue
		}
		if s.requireAnswered && !q.IsAnswered {
			continue
		}
		keepers = append(keepers, q)
	}
	if len(keepers) == 0 {
		return nil, oldest, false, nil
	}

	ids := make([]string, 0, len(keepers))
	for _, q := range keepers {
		ids = append(ids, strconv.Itoa(q.QuestionID))
	}
	idList := strings.Join(ids, ";")

	// batched bodies call
	bp := url.Values{"site": {s.site}, "filter": {"withbody"}, "pagesize": {"100"}}
	var full []seQuestion
	if err := s.getJSON(ctx, s.buildURL("/questions/"+idList, bp), &full); err != nil {
		return nil, time.Time{}, false, err
	}
	bodies := map[int]string{}
	for _, q := range full {
		bodies[q.QuestionID] = q.Body
	}

	// batched answers call (ids are ';'-joined)
	ap := url.Values{
		"order":    {"desc"},
		"sort":     {"votes"},
		"site":     {s.site},
		"pagesize": {"100"},
		"filter":   {"withbody"},
	}
	answers := map[int][]seAnswer{}
	var items []seAnswer
	if err := s.getJSON(ctx, s.buildURL("/questions/"+idList+"/answers", ap), &items); err != nil {
		return nil, time.Time{}, false, err
	}
	for _, a := range items {
		answers[a.QuestionID] = append(answers[a.QuestionID], a)
	}

	out := make([]ArticleRaw, 0, len(keepers))
	for _, q := range keepers {
		q.Body = bodies[q.QuestionID]
		content, ok := s.composeDoc(q, answers[q.QuestionID])
		if !ok {
			continue // unconvertible: editorially skipped
		}
		payload, _ := json.Marshal(map[string]any{
			"question_id": q.QuestionID,
			"score":       q.Score,
			"site":        s.site,
		})
		out = append(out, ArticleRaw{
			URL:        q.Link,
			Title:      html.UnescapeString(q.Title),
			Author:     html.UnescapeString(q.Owner.DisplayName),
			Published:  time.Unix(q.CreationDate, 0).UTC(),
			Tags:       q.Tags,
			SourceType: "stackexchange",
			Payload:    payload,
			ContentMD:  content,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Published.After(out[j].Published) })
	return out, oldest, false, nil
}
