package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration
type Config struct {
	Zulip struct {
		Site  string `yaml:"site"`
		Email string `yaml:"email"`
		Key   string `yaml:"key"`
	} `yaml:"zulip"`
	Telegram struct {
		BotToken  string `yaml:"bot_token"`
		ChannelID string `yaml:"channel_id"`
	} `yaml:"telegram"`
	Filters struct {
		Streams     []string `yaml:"streams"`
		Topics      []string `yaml:"topics"`
		InvertLogic bool     `yaml:"invert_logic"`
	} `yaml:"filters,omitempty"`
	RateLimit struct {
		MessagesPerSecond int           `yaml:"messages_per_second"`
		Burst             int           `yaml:"burst"`
		MaxRetries        int           `yaml:"max_retries"`
		InitialBackoff    time.Duration `yaml:"initial_backoff"`
		MaxBackoff        time.Duration `yaml:"max_backoff"`
	} `yaml:"rate_limit,omitempty"`
	Attachments struct {
		Enabled         bool          `yaml:"enabled"`
		MaxSizeMB       int           `yaml:"max_size_mb"`
		DownloadTimeout time.Duration `yaml:"download_timeout"`
	} `yaml:"attachments,omitempty"`
	// Message formatting options
	MessageFormat struct {
		// Include Zulip message link (default: false)
		IncludeZulipLink bool `yaml:"include_zulip_link"`
	} `yaml:"message_format,omitempty"`
}

// LoadConfig reads configuration from YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if cfg.RateLimit.MessagesPerSecond == 0 {
		cfg.RateLimit.MessagesPerSecond = 20
	}
	if cfg.RateLimit.Burst == 0 {
		cfg.RateLimit.Burst = 5
	}
	if cfg.RateLimit.MaxRetries == 0 {
		cfg.RateLimit.MaxRetries = 3
	}
	if cfg.RateLimit.InitialBackoff == 0 {
		cfg.RateLimit.InitialBackoff = 1 * time.Second
	}
	if cfg.RateLimit.MaxBackoff == 0 {
		cfg.RateLimit.MaxBackoff = 60 * time.Second
	}
	if cfg.Attachments.MaxSizeMB == 0 {
		cfg.Attachments.MaxSizeMB = 20
	}
	if cfg.Attachments.DownloadTimeout == 0 {
		cfg.Attachments.DownloadTimeout = 30 * time.Second
	}
	// MessageFormat.IncludeZulipLink defaults to false (zero value)

	// Validate required fields
	if cfg.Zulip.Site == "" || cfg.Zulip.Email == "" || cfg.Zulip.Key == "" {
		return nil, fmt.Errorf("zulip configuration incomplete: site, email, and key are required")
	}
	if cfg.Telegram.BotToken == "" || cfg.Telegram.ChannelID == "" {
		return nil, fmt.Errorf("telegram configuration incomplete: bot_token and channel_id are required")
	}

	return &cfg, nil
}
