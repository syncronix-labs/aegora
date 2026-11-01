// Package config provides runtime configuration loading and validation
// for the Aegora gateway.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration with YAML unmarshaling support
// for duration strings like "30s", "5m", "100ms".
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// String returns the string representation of the duration.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("decode duration: %w", err)
	}

	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}

	*d = Duration(dur)
	return nil
}

// Config represents the top-level configuration for Aegora.
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Routes        []RouteConfig       `yaml:"routes"`
	Observability ObservabilityConfig `yaml:"observability"`
	RateLimit     RateLimitConfig     `yaml:"rate_limit,omitempty"`
	Reliability   ReliabilityConfig   `yaml:"reliability,omitempty"`
	Queue         QueueConfig         `yaml:"queue,omitempty"`
	Shedding      SheddingConfig      `yaml:"shedding,omitempty"`
	Cache         CacheConfig         `yaml:"cache,omitempty"`
	Admin         AdminConfig         `yaml:"admin,omitempty"`
}

// AdminConfig configures the dedicated administrative HTTP server.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

// CacheConfig configures HTTP response caching for GET and HEAD requests.
// Supports multi-tier caching (L1 in-memory + optional L2 Redis).
type CacheConfig struct {
	Enabled      bool     `yaml:"enabled"`
	TTL          Duration `yaml:"ttl,omitempty"`
	L2Enabled    bool     `yaml:"l2_enabled,omitempty"`
	L2TTL        Duration `yaml:"l2_ttl,omitempty"`
	RedisAddr    string   `yaml:"redis_addr,omitempty"`
	MaxEntries   int      `yaml:"max_entries,omitempty"`
	MaxBodyBytes int      `yaml:"max_body_bytes,omitempty"`
}

// SheddingConfig configures adaptive load shedding with an AIMD controller.
// Note: priority_paths may be shared with the queue section in a future unified
// classification model; for now, each section owns its independent copy.
type SheddingConfig struct {
	Enabled        bool                `yaml:"enabled"`
	TargetP99      Duration            `yaml:"target_p99,omitempty"`
	MinAdmitRate   float64             `yaml:"min_admit_rate,omitempty"`
	AdjustInterval Duration            `yaml:"adjust_interval,omitempty"`
	PriorityPaths  PriorityPathsConfig `yaml:"priority_paths,omitempty"`
}

// PriorityPathsConfig configures path prefixes mapped to request priorities.
type PriorityPathsConfig struct {
	High []string `yaml:"high,omitempty"`
	Low  []string `yaml:"low,omitempty"`
}

// QueueConfig holds admission-control queue and CoDel configuration.
type QueueConfig struct {
	Enabled       bool                `yaml:"enabled"`
	MaxInflight   int                 `yaml:"max_inflight,omitempty"`
	MaxQueueDepth int                 `yaml:"max_queue_depth,omitempty"`
	CodelTarget   Duration            `yaml:"codel_target,omitempty"`
	CodelInterval Duration            `yaml:"codel_interval,omitempty"`
	PriorityPaths PriorityPathsConfig `yaml:"priority_paths,omitempty"`
}

// ReliabilityConfig holds circuit breaking and retry configuration.
type ReliabilityConfig struct {
	BreakerFailureThreshold int      `yaml:"breaker_failure_threshold,omitempty"`
	BreakerCooldown         Duration `yaml:"breaker_cooldown,omitempty"`
	RetryEnabled            bool     `yaml:"retry_enabled,omitempty"`
	RetryMaxAttempts        int      `yaml:"retry_max_attempts,omitempty"`
	RetryInitialDelay       Duration `yaml:"retry_initial_delay,omitempty"`
}

// RateLimitConfig configures gateway request rate limiting behavior.
type RateLimitConfig struct {
	Enabled         bool     `yaml:"enabled"`
	Limiter         string   `yaml:"limiter,omitempty"`
	MaxRequests     int      `yaml:"max_requests,omitempty"`
	Window          Duration `yaml:"window,omitempty"`
	RefillPerSecond float64  `yaml:"refill_per_second,omitempty"`
	Key             string   `yaml:"key,omitempty"`
}

// RouteRateLimitConfig overrides the global rate_limit settings for a single route.
// Fields are pointers so an omitted field (nil) is distinguishable from an explicit
// zero or false: nil fields are inherited from the global rate_limit block.
type RouteRateLimitConfig struct {
	Enabled         *bool     `yaml:"enabled,omitempty"`
	Limiter         *string   `yaml:"limiter,omitempty"`
	MaxRequests     *int      `yaml:"max_requests,omitempty"`
	Window          *Duration `yaml:"window,omitempty"`
	RefillPerSecond *float64  `yaml:"refill_per_second,omitempty"`
	Key             *string   `yaml:"key,omitempty"`
}

// isEmpty reports whether no field is set. An empty block is treated as no override,
// so it does not silently create a second set of buckets for the route.
func (o *RouteRateLimitConfig) isEmpty() bool {
	return o == nil || (o.Enabled == nil && o.Limiter == nil && o.MaxRequests == nil &&
		o.Window == nil && o.RefillPerSecond == nil && o.Key == nil)
}

// HasRateLimitOverride reports whether the route defines a non-empty rate_limit override.
func (r RouteConfig) HasRateLimitOverride() bool {
	return !r.RateLimit.isEmpty()
}

// EffectiveRateLimit returns the rate limit settings that apply to route: the global
// rate_limit block with every field set in the route's override replacing its value.
func (c *Config) EffectiveRateLimit(route RouteConfig) RateLimitConfig {
	rl := c.RateLimit
	o := route.RateLimit
	if o.isEmpty() {
		return rl
	}
	if o.Enabled != nil {
		rl.Enabled = *o.Enabled
	}
	if o.Limiter != nil {
		rl.Limiter = *o.Limiter
	}
	if o.MaxRequests != nil {
		rl.MaxRequests = *o.MaxRequests
	}
	if o.Window != nil {
		rl.Window = *o.Window
	}
	if o.RefillPerSecond != nil {
		rl.RefillPerSecond = *o.RefillPerSecond
	}
	if o.Key != nil {
		rl.Key = *o.Key
	}
	return rl
}

// ServerConfig holds HTTP server network and timeout settings.
type ServerConfig struct {
	Addr         string   `yaml:"addr"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
	IdleTimeout  Duration `yaml:"idle_timeout"`
	// MaxRequestBodyBytes caps the size of incoming request bodies.
	// Zero (the default) means unlimited.
	MaxRequestBodyBytes int64 `yaml:"max_request_body_bytes,omitempty"`
}

// RouteConfig specifies upstream routing rules and target destinations.
type RouteConfig struct {
	Name        string            `yaml:"name"`
	PathPrefix  string            `yaml:"path_prefix"`
	PathRegex   string            `yaml:"path_regex,omitempty"`
	Methods     []string          `yaml:"methods,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty"`
	Targets     []string          `yaml:"targets"`
	StripPrefix string            `yaml:"strip_prefix,omitempty"`
	LB          string            `yaml:"lb,omitempty"`
	// Timeout optionally bounds the whole outbound exchange for this route.
	// Zero means no per-route timeout.
	Timeout Duration `yaml:"timeout,omitempty"`
	// RateLimit optionally overrides the global rate_limit block for this route.
	RateLimit *RouteRateLimitConfig `yaml:"rate_limit,omitempty"`
}

// TracingConfig configures OpenTelemetry distributed tracing.
type TracingConfig struct {
	Enabled      bool    `yaml:"enabled"`
	Exporter     string  `yaml:"exporter,omitempty"`
	OTLPEndpoint string  `yaml:"otlp_endpoint,omitempty"`
	SampleRatio  float64 `yaml:"sample_ratio,omitempty"`
}

// ObservabilityConfig contains telemetry and logging configuration.
type ObservabilityConfig struct {
	LogLevel    string        `yaml:"log_level"`
	MetricsAddr string        `yaml:"metrics_addr"`
	Tracing     TracingConfig `yaml:"tracing,omitempty"`
}

// Load reads a YAML configuration file from path, decodes it strictly,
// applies environment variable overrides, and validates the resulting configuration.
// A missing or unreadable file returns an error.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file: %w", err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	if err := applyEnvOverrides(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// applyEnvOverrides applies environment variable overrides using an explicit allowlist.
// Environment variables take precedence over file-configured values.
func applyEnvOverrides(cfg *Config) error {
	if val, ok := os.LookupEnv("AEGORA_SERVER_ADDR"); ok {
		cfg.Server.Addr = val
	}

	if val, ok := os.LookupEnv("AEGORA_SERVER_READ_TIMEOUT"); ok {
		dur, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("config: env AEGORA_SERVER_READ_TIMEOUT: invalid duration %q: %w", val, err)
		}
		cfg.Server.ReadTimeout = Duration(dur)
	}

	if val, ok := os.LookupEnv("AEGORA_SERVER_WRITE_TIMEOUT"); ok {
		dur, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("config: env AEGORA_SERVER_WRITE_TIMEOUT: invalid duration %q: %w", val, err)
		}
		cfg.Server.WriteTimeout = Duration(dur)
	}

	if val, ok := os.LookupEnv("AEGORA_SERVER_IDLE_TIMEOUT"); ok {
		dur, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("config: env AEGORA_SERVER_IDLE_TIMEOUT: invalid duration %q: %w", val, err)
		}
		cfg.Server.IdleTimeout = Duration(dur)
	}

	if val, ok := os.LookupEnv("AEGORA_OBSERVABILITY_LOG_LEVEL"); ok {
		cfg.Observability.LogLevel = val
	}

	if val, ok := os.LookupEnv("AEGORA_OBSERVABILITY_METRICS_ADDR"); ok {
		cfg.Observability.MetricsAddr = val
	}

	if val, ok := os.LookupEnv("AEGORA_ADMIN_ENABLED"); ok {
		cfg.Admin.Enabled = strings.ToLower(strings.TrimSpace(val)) == "true" || val == "1"
	}

	if val, ok := os.LookupEnv("AEGORA_ADMIN_ADDR"); ok {
		cfg.Admin.Addr = val
	}

	return nil
}

// Validate checks the configuration for correctness and enforces fail-fast rules.
// Every error identifies the offending field.
func (c *Config) Validate() error {
	if _, err := net.ResolveTCPAddr("tcp", c.Server.Addr); err != nil {
		return fmt.Errorf("config: server.addr: invalid address %q: %w", c.Server.Addr, err)
	}

	if c.Server.ReadTimeout <= 0 {
		return fmt.Errorf("config: server.read_timeout: must be positive, got %s", c.Server.ReadTimeout)
	}

	if c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("config: server.write_timeout: must be positive, got %s", c.Server.WriteTimeout)
	}

	if c.Server.IdleTimeout <= 0 {
		return fmt.Errorf("config: server.idle_timeout: must be positive, got %s", c.Server.IdleTimeout)
	}

	if c.Server.MaxRequestBodyBytes < 0 {
		return fmt.Errorf("config: server.max_request_body_bytes: must be non-negative, got %d", c.Server.MaxRequestBodyBytes)
	}

	if len(c.Routes) == 0 {
		return fmt.Errorf("config: routes: must configure at least one route")
	}

	seenNames := make(map[string]struct{}, len(c.Routes))
	for i := range c.Routes {
		route := &c.Routes[i]

		if strings.TrimSpace(route.Name) == "" {
			return fmt.Errorf("config: routes[%d].name: cannot be empty", i)
		}

		if _, exists := seenNames[route.Name]; exists {
			return fmt.Errorf("config: routes[%d].name: duplicate route name %q", i, route.Name)
		}
		seenNames[route.Name] = struct{}{}

		if len(route.Targets) == 0 {
			return fmt.Errorf("config: routes[%d].targets: must configure at least one target", i)
		}

		for j, target := range route.Targets {
			u, err := url.ParseRequestURI(target)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("config: routes[%d].targets[%d]: invalid URL %q", i, j, target)
			}
		}

		if route.StripPrefix != "" && !strings.HasPrefix(route.StripPrefix, "/") {
			return fmt.Errorf("config: routes[%d].strip_prefix: must start with %q, got %q", i, "/", route.StripPrefix)
		}

		if route.PathRegex != "" {
			if _, err := regexp.Compile(route.PathRegex); err != nil {
				return fmt.Errorf("config: routes[%d].path_regex: invalid regex %q: %w", i, route.PathRegex, err)
			}
		}

		if route.LB == "" {
			route.LB = "round_robin"
		} else if route.LB != "round_robin" && route.LB != "least_connections" {
			return fmt.Errorf("config: routes[%d].lb: must be one of round_robin, least_connections; got %q", i, route.LB)
		}
	}

	switch c.Observability.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: observability.log_level: must be one of debug, info, warn, error; got %q", c.Observability.LogLevel)
	}

	if c.Observability.MetricsAddr != "" {
		if _, err := net.ResolveTCPAddr("tcp", c.Observability.MetricsAddr); err != nil {
			return fmt.Errorf("config: observability.metrics_addr: invalid address %q: %w", c.Observability.MetricsAddr, err)
		}
	}

	if c.Observability.Tracing.Enabled {
		if c.Observability.Tracing.Exporter != "stdout" && c.Observability.Tracing.Exporter != "otlp" {
			return fmt.Errorf("config: observability.tracing.exporter: must be one of stdout, otlp; got %q", c.Observability.Tracing.Exporter)
		}
		if c.Observability.Tracing.SampleRatio <= 0.0 || c.Observability.Tracing.SampleRatio > 1.0 {
			return fmt.Errorf("config: observability.tracing.sample_ratio: must be in (0, 1], got %f", c.Observability.Tracing.SampleRatio)
		}
		if c.Observability.Tracing.Exporter == "otlp" && strings.TrimSpace(c.Observability.Tracing.OTLPEndpoint) == "" {
			return errors.New("config: observability.tracing.otlp_endpoint: required when exporter is otlp")
		}
	}

	if c.Admin.Enabled {
		if strings.TrimSpace(c.Admin.Addr) == "" {
			return errors.New("config: admin.addr: required when admin is enabled")
		}
		if _, err := net.ResolveTCPAddr("tcp", c.Admin.Addr); err != nil {
			return fmt.Errorf("config: admin.addr: invalid address %q: %w", c.Admin.Addr, err)
		}
	}

	if err := validateRateLimit("rate_limit", c.RateLimit); err != nil {
		return err
	}

	// Per-route overrides are validated on the merged result, so an override that is
	// only valid in combination with inherited global fields is accepted, and errors
	// name the route that produced them.
	for i, route := range c.Routes {
		if route.Timeout < 0 {
			return fmt.Errorf("config: routes[%d].timeout: must be non-negative, got %s", i, route.Timeout)
		}
		if !route.HasRateLimitOverride() {
			continue
		}
		if err := validateRateLimit(fmt.Sprintf("routes[%d].rate_limit", i), c.EffectiveRateLimit(route)); err != nil {
			return err
		}
	}

	if c.isReliabilityPresent() {
		if c.Reliability.BreakerFailureThreshold < 1 {
			return fmt.Errorf("config: reliability.breaker_failure_threshold: must be at least 1, got %d", c.Reliability.BreakerFailureThreshold)
		}

		if c.Reliability.BreakerCooldown <= 0 {
			return fmt.Errorf("config: reliability.breaker_cooldown: must be positive, got %s", c.Reliability.BreakerCooldown)
		}

		if c.Reliability.RetryEnabled || c.Reliability.RetryMaxAttempts != 0 || c.Reliability.RetryInitialDelay != 0 {
			if c.Reliability.RetryMaxAttempts < 1 {
				return fmt.Errorf("config: reliability.retry_max_attempts: must be at least 1, got %d", c.Reliability.RetryMaxAttempts)
			}

			if c.Reliability.RetryInitialDelay <= 0 {
				return fmt.Errorf("config: reliability.retry_initial_delay: must be positive, got %s", c.Reliability.RetryInitialDelay)
			}
		}
	}

	if c.Queue.Enabled {
		if c.Queue.MaxInflight <= 0 {
			return fmt.Errorf("config: queue.max_inflight: must be positive, got %d", c.Queue.MaxInflight)
		}
		if c.Queue.MaxQueueDepth <= 0 {
			return fmt.Errorf("config: queue.max_queue_depth: must be positive, got %d", c.Queue.MaxQueueDepth)
		}
		if c.Queue.CodelTarget <= 0 {
			return fmt.Errorf("config: queue.codel_target: must be positive, got %s", c.Queue.CodelTarget)
		}
		if c.Queue.CodelInterval <= 0 {
			return fmt.Errorf("config: queue.codel_interval: must be positive, got %s", c.Queue.CodelInterval)
		}
	}

	if c.Shedding.Enabled {
		if c.Shedding.TargetP99 <= 0 {
			return fmt.Errorf("config: shedding.target_p99: must be positive, got %s", c.Shedding.TargetP99)
		}
		if c.Shedding.MinAdmitRate <= 0.0 || c.Shedding.MinAdmitRate >= 1.0 {
			return fmt.Errorf("config: shedding.min_admit_rate: must be between 0.0 and 1.0 (exclusive), got %f", c.Shedding.MinAdmitRate)
		}
		if c.Shedding.AdjustInterval <= 0 {
			return fmt.Errorf("config: shedding.adjust_interval: must be positive, got %s", c.Shedding.AdjustInterval)
		}
	}

	if c.Cache.Enabled {
		if c.Cache.TTL <= 0 {
			return fmt.Errorf("config: cache.ttl: must be positive, got %s", c.Cache.TTL)
		}
		if c.Cache.MaxEntries <= 0 {
			return fmt.Errorf("config: cache.max_entries: must be positive, got %d", c.Cache.MaxEntries)
		}
		if c.Cache.MaxBodyBytes <= 0 {
			return fmt.Errorf("config: cache.max_body_bytes: must be positive, got %d", c.Cache.MaxBodyBytes)
		}
	}

	if c.Cache.L2Enabled {
		if strings.TrimSpace(c.Cache.RedisAddr) == "" {
			return errors.New("config: cache.redis_addr: required when cache.l2_enabled is true")
		}
		if c.Cache.L2TTL <= 0 {
			return fmt.Errorf("config: cache.l2_ttl: must be positive, got %s", c.Cache.L2TTL)
		}
	}

	return nil
}

// validateRateLimit checks a rate limit block. prefix names the block in error messages,
// e.g. "rate_limit" for the global block or "routes[2].rate_limit" for a merged override.
func validateRateLimit(prefix string, rl RateLimitConfig) error {
	if !rl.Enabled {
		return nil
	}

	if rl.MaxRequests <= 0 {
		return fmt.Errorf("config: %s.max_requests: must be positive, got %d", prefix, rl.MaxRequests)
	}

	if rl.Limiter != "sliding_window" && rl.Limiter != "token_bucket" {
		return fmt.Errorf("config: %s.limiter: must be one of sliding_window, token_bucket; got %q", prefix, rl.Limiter)
	}

	if rl.Key != "ip" && rl.Key != "api_key" {
		return fmt.Errorf("config: %s.key: must be one of ip, api_key; got %q", prefix, rl.Key)
	}

	if rl.Limiter == "sliding_window" && rl.Window <= 0 {
		return fmt.Errorf("config: %s.window: must be positive for sliding_window, got %s", prefix, rl.Window)
	}

	if rl.Limiter == "token_bucket" && rl.RefillPerSecond <= 0 {
		return fmt.Errorf("config: %s.refill_per_second: must be positive for token_bucket, got %f", prefix, rl.RefillPerSecond)
	}

	return nil
}

func (c *Config) isReliabilityPresent() bool {
	return c.Reliability.BreakerFailureThreshold != 0 ||
		c.Reliability.BreakerCooldown != 0 ||
		c.Reliability.RetryEnabled ||
		c.Reliability.RetryMaxAttempts != 0 ||
		c.Reliability.RetryInitialDelay != 0
}
