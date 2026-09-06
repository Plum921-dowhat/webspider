package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type RedisConfig struct {
	Addr          string `yaml:"addr"`
	Password      string `yaml:"password"`
	Stream        string `yaml:"stream"`
	ConsumerGroup string `yaml:"consumer_group"`
	// StreamMaxLen caps the stream with XADD MAXLEN ~ N. 0 = unbounded.
	StreamMaxLen int `yaml:"stream_max_len"`
}

type CrawlerConfig struct {
	Source          string `yaml:"source"`
	PerPage         int    `yaml:"per_page"`
	MaxConcurrency  int    `yaml:"max_concurrency"`
	RatePerSec      int    `yaml:"rate_per_sec"`
	SinceHours      int    `yaml:"since_hours"`
	MaxPages        int    `yaml:"max_pages"`
	IntervalMinutes int    `yaml:"interval_minutes"` // >0: run on a loop; 0: run once
	// MaxPageFailures aborts a run after this many failed pages (circuit
	// breaker against a down/blocked upstream). <=0 falls back to the default.
	MaxPageFailures int `yaml:"max_page_failures"`
	// UserAgent overrides the bot UA sent to upstreams; CRAWLER_USER_AGENT
	// env wins. Fill in a real contact address for long-running deployments.
	UserAgent string `yaml:"user_agent"`

	// --- source-specific options ---

	// RSS: feed URLs polled once per run (page > 1 is empty by design).
	RSSFeeds []string `yaml:"rss_feeds"`
	// Stack Exchange: site slug and editorial filters; the API key comes from
	// env SE_API_KEY (yaml se_api_key is the fallback).
	SESite            string `yaml:"se_site"`
	SEMinScore        int    `yaml:"se_min_score"`
	SERequireAnswered bool   `yaml:"se_require_answered"`
	SEAPIKey          string `yaml:"se_api_key"`
}

type Config struct {
	Redis   RedisConfig   `yaml:"redis"`
	Crawler CrawlerConfig `yaml:"crawler"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&c)
	return &c, nil
}

func (c *Config) Since() time.Time {
	if c.Crawler.SinceHours <= 0 {
		return time.Time{}
	}
	return time.Now().Add(-time.Duration(c.Crawler.SinceHours) * time.Hour)
}

func applyDefaults(c *Config) {
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	if c.Redis.Stream == "" {
		c.Redis.Stream = "articles"
	}
	if c.Redis.ConsumerGroup == "" {
		c.Redis.ConsumerGroup = "py"
	}
	if c.Crawler.PerPage <= 0 {
		c.Crawler.PerPage = 30
	}
	if c.Crawler.MaxConcurrency <= 0 {
		c.Crawler.MaxConcurrency = 8
	}
	if c.Crawler.RatePerSec <= 0 {
		c.Crawler.RatePerSec = 5
	}
	if c.Crawler.MaxPageFailures <= 0 {
		c.Crawler.MaxPageFailures = 3
	}
	if c.Redis.StreamMaxLen <= 0 {
		c.Redis.StreamMaxLen = 100000
	}
}
