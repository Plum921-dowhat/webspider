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
}

type CrawlerConfig struct {
	Source         string `yaml:"source"`
	PerPage        int    `yaml:"per_page"`
	MaxConcurrency int    `yaml:"max_concurrency"`
	RatePerSec     int    `yaml:"rate_per_sec"`
	SinceHours     int    `yaml:"since_hours"`
	MaxPages       int    `yaml:"max_pages"`
}

type Config struct {
	Redis    RedisConfig    `yaml:"redis"`
	Crawler  CrawlerConfig  `yaml:"crawler"`
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
}
