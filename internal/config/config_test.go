package config_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/syncronix-labs/aegora/internal/config"
	"gopkg.in/yaml.v3"
)

func TestLoad_Valid(t *testing.T) {
	cfg, err := config.Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("config.Load() unexpected error: %v", err)
	}

	expected := &config.Config{
		Server: config.ServerConfig{
			Addr:         "127.0.0.1:8080",
			ReadTimeout:  config.Duration(15 * time.Second),
			WriteTimeout: config.Duration(30 * time.Second),
			IdleTimeout:  config.Duration(60 * time.Second),
		},
		Routes: []config.RouteConfig{
			{
				Name:       "api",
				PathPrefix: "/api/v1",
				Targets: []string{
					"http://127.0.0.1:8081",
					"http://127.0.0.1:8082",
				},
				StripPrefix: "/api/v1",
				LB:          "round_robin",
			},
			{
				Name:        "auth",
				PathPrefix:  "/auth",
				Targets:     []string{"https://auth.example.com"},
				StripPrefix: "",
				LB:          "round_robin",
			},
		},
		Observability: config.ObservabilityConfig{
			LogLevel:    "debug",
			MetricsAddr: "127.0.0.1:9090",
		},
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Fatalf("config mismatch:\ngot:  %+v\nwant: %+v", cfg, expected)
	}

	if got := cfg.Server.ReadTimeout.Duration(); got != 15*time.Second {
		t.Fatalf("expected ReadTimeout 15s, got %v", got)
	}
	if got := cfg.Server.WriteTimeout.Duration(); got != 30*time.Second {
		t.Fatalf("expected WriteTimeout 30s, got %v", got)
	}
	if got := cfg.Server.IdleTimeout.Duration(); got != 60*time.Second {
		t.Fatalf("expected IdleTimeout 60s, got %v", got)
	}
	if got := cfg.Server.ReadTimeout.String(); got != "15s" {
		t.Fatalf("expected ReadTimeout string %q, got %q", "15s", got)
	}
}

func TestLoad_ParseLevelFailures(t *testing.T) {
	tests := []struct {
		name        string
		fixture     string
		wantErrText string
	}{
		{
			name:        "invalid duration in YAML",
			fixture:     "testdata/invalid-duration.yaml",
			wantErrText: "invalid duration",
		},
		{
			name:        "unknown field in YAML",
			fixture:     "testdata/invalid-unknown-field.yaml",
			wantErrText: "extra_typo_field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Load(tt.fixture)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func parseValidFixture(t *testing.T) *config.Config {
	t.Helper()

	data, err := os.ReadFile("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("failed to read testdata/valid.yaml: %v", err)
	}

	var cfg config.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("failed to unmarshal testdata/valid.yaml: %v", err)
	}

	return &cfg
}

func TestValidate_MutatedNegativeCases(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(c *config.Config)
		wantFieldInErr string
	}{
		{
			name: "malformed server addr without port",
			mutate: func(c *config.Config) {
				c.Server.Addr = "invalid_addr_no_port"
			},
			wantFieldInErr: "server.addr",
		},
		{
			name: "zero read timeout",
			mutate: func(c *config.Config) {
				c.Server.ReadTimeout = 0
			},
			wantFieldInErr: "server.read_timeout",
		},
		{
			name: "negative write timeout",
			mutate: func(c *config.Config) {
				c.Server.WriteTimeout = -1
			},
			wantFieldInErr: "server.write_timeout",
		},
		{
			name: "zero idle timeout",
			mutate: func(c *config.Config) {
				c.Server.IdleTimeout = 0
			},
			wantFieldInErr: "server.idle_timeout",
		},
		{
			name: "negative max request body bytes",
			mutate: func(c *config.Config) {
				c.Server.MaxRequestBodyBytes = -1
			},
			wantFieldInErr: "server.max_request_body_bytes",
		},
		{
			name: "zero routes configured",
			mutate: func(c *config.Config) {
				c.Routes = nil
			},
			wantFieldInErr: "routes",
		},
		{
			name: "empty route name",
			mutate: func(c *config.Config) {
				c.Routes[0].Name = ""
			},
			wantFieldInErr: "routes[0].name",
		},
		{
			name: "duplicate route name",
			mutate: func(c *config.Config) {
				c.Routes = append(c.Routes, config.RouteConfig{
					Name:        c.Routes[0].Name,
					PathPrefix:  "/dup",
					Targets:     []string{"http://127.0.0.1:8080"},
					StripPrefix: "",
				})
			},
			wantFieldInErr: "routes[2].name",
		},
		{
			name: "route with no targets",
			mutate: func(c *config.Config) {
				c.Routes[0].Targets = nil
			},
			wantFieldInErr: "routes[0].targets",
		},
		{
			name: "invalid target URL",
			mutate: func(c *config.Config) {
				c.Routes[0].Targets = []string{"/relative/url"}
			},
			wantFieldInErr: "routes[0].targets[0]",
		},
		{
			name: "strip_prefix not starting with slash",
			mutate: func(c *config.Config) {
				c.Routes[0].StripPrefix = "api/v1"
			},
			wantFieldInErr: "routes[0].strip_prefix",
		},
		{
			name: "bad log level",
			mutate: func(c *config.Config) {
				c.Observability.LogLevel = "verbose"
			},
			wantFieldInErr: "observability.log_level",
		},
		{
			name: "invalid path_regex",
			mutate: func(c *config.Config) {
				c.Routes[0].PathRegex = "[invalid-(regex"
			},
			wantFieldInErr: "routes[0].path_regex",
		},
		{
			name: "invalid lb policy",
			mutate: func(c *config.Config) {
				c.Routes[0].LB = "random"
			},
			wantFieldInErr: "routes[0].lb",
		},
		{
			name: "negative route timeout",
			mutate: func(c *config.Config) {
				c.Routes[0].Timeout = config.Duration(-1)
			},
			wantFieldInErr: "routes[0].timeout",
		},
		{
			name: "invalid metrics_addr missing port",
			mutate: func(c *config.Config) {
				c.Observability.MetricsAddr = "127.0.0.1"
			},
			wantFieldInErr: "observability.metrics_addr",
		},
		{
			name: "invalid metrics_addr host port",
			mutate: func(c *config.Config) {
				c.Observability.MetricsAddr = "not-a-valid-address"
			},
			wantFieldInErr: "observability.metrics_addr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantFieldInErr)
			}
			if !strings.Contains(err.Error(), tt.wantFieldInErr) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantFieldInErr, err)
			}
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load("testdata/nonexistent-file.yaml")
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
	if !strings.Contains(err.Error(), "config: read file") {
		t.Fatalf("expected error to mention reading file, got: %v", err)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("AEGORA_SERVER_ADDR", "127.0.0.1:9999")
	t.Setenv("AEGORA_SERVER_READ_TIMEOUT", "45s")
	t.Setenv("AEGORA_SERVER_WRITE_TIMEOUT", "55s")
	t.Setenv("AEGORA_SERVER_IDLE_TIMEOUT", "120s")
	t.Setenv("AEGORA_OBSERVABILITY_LOG_LEVEL", "warn")
	t.Setenv("AEGORA_OBSERVABILITY_METRICS_ADDR", "127.0.0.1:9191")
	t.Setenv("AEGORA_ADMIN_ENABLED", "true")
	t.Setenv("AEGORA_ADMIN_ADDR", "127.0.0.1:9192")

	cfg, err := config.Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("unexpected error loading with env overrides: %v", err)
	}

	if cfg.Server.Addr != "127.0.0.1:9999" {
		t.Errorf("server.addr: got %q, want %q", cfg.Server.Addr, "127.0.0.1:9999")
	}
	if cfg.Server.ReadTimeout.Duration() != 45*time.Second {
		t.Errorf("server.read_timeout: got %v, want %v", cfg.Server.ReadTimeout.Duration(), 45*time.Second)
	}
	if cfg.Server.WriteTimeout.Duration() != 55*time.Second {
		t.Errorf("server.write_timeout: got %v, want %v", cfg.Server.WriteTimeout.Duration(), 55*time.Second)
	}
	if cfg.Server.IdleTimeout.Duration() != 120*time.Second {
		t.Errorf("server.idle_timeout: got %v, want %v", cfg.Server.IdleTimeout.Duration(), 120*time.Second)
	}
	if cfg.Observability.LogLevel != "warn" {
		t.Errorf("observability.log_level: got %q, want %q", cfg.Observability.LogLevel, "warn")
	}
	if cfg.Observability.MetricsAddr != "127.0.0.1:9191" {
		t.Errorf("observability.metrics_addr: got %q, want %q", cfg.Observability.MetricsAddr, "127.0.0.1:9191")
	}
	if !cfg.Admin.Enabled {
		t.Errorf("admin.enabled: got false, want true")
	}
	if cfg.Admin.Addr != "127.0.0.1:9192" {
		t.Errorf("admin.addr: got %q, want %q", cfg.Admin.Addr, "127.0.0.1:9192")
	}
}

func TestLoad_EnvOverride_InvalidDuration(t *testing.T) {
	t.Setenv("AEGORA_SERVER_READ_TIMEOUT", "invalid-duration")

	_, err := config.Load("testdata/valid.yaml")
	if err == nil {
		t.Fatal("expected error for invalid duration env override, got nil")
	}
	if !strings.Contains(err.Error(), "AEGORA_SERVER_READ_TIMEOUT") {
		t.Fatalf("expected error to mention env var name, got: %v", err)
	}
}

func TestDuration_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{
			name:    "valid 30s",
			input:   "30s",
			want:    30 * time.Second,
			wantErr: false,
		},
		{
			name:    "valid 1m30s",
			input:   "1m30s",
			want:    90 * time.Second,
			wantErr: false,
		},
		{
			name:    "valid 500ms",
			input:   "500ms",
			want:    500 * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "invalid bogus duration",
			input:   "bogus",
			wantErr: true,
		},
		{
			name:    "invalid mapping duration",
			input:   "{foo: bar}",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d config.Duration
			err := yaml.Unmarshal([]byte(tt.input), &d)
			if (err != nil) != tt.wantErr {
				t.Fatalf("yaml.Unmarshal(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && d.Duration() != tt.want {
				t.Fatalf("yaml.Unmarshal(%q) = %v, want %v", tt.input, d.Duration(), tt.want)
			}
		})
	}
}

func TestLoad_SampleDevConfig(t *testing.T) {
	cfg, err := config.Load("../../configs/aegora.yaml")
	if err != nil {
		t.Fatalf("failed to load configs/aegora.yaml: %v", err)
	}

	if cfg.Server.Addr != "127.0.0.1:8080" {
		t.Errorf("unexpected server.addr: %s", cfg.Server.Addr)
	}
	if len(cfg.Routes) != 2 {
		t.Errorf("expected 2 routes, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].LB != "round_robin" {
		t.Errorf("expected route 0 LB round_robin, got %s", cfg.Routes[0].LB)
	}
	if cfg.Routes[1].LB != "least_connections" {
		t.Errorf("expected route 1 LB least_connections, got %s", cfg.Routes[1].LB)
	}
	if !reflect.DeepEqual(cfg.Routes[0].Methods, []string{"GET", "POST"}) {
		t.Errorf("expected route 0 methods [GET POST], got %v", cfg.Routes[0].Methods)
	}

	if !cfg.RateLimit.Enabled {
		t.Errorf("expected rate_limit.enabled = true")
	}
	if cfg.RateLimit.Limiter != "sliding_window" {
		t.Errorf("expected rate_limit.limiter = sliding_window, got %s", cfg.RateLimit.Limiter)
	}
	if cfg.RateLimit.MaxRequests != 60 {
		t.Errorf("expected rate_limit.max_requests = 60, got %d", cfg.RateLimit.MaxRequests)
	}
	if cfg.RateLimit.Window.Duration() != time.Minute {
		t.Errorf("expected rate_limit.window = 1m, got %s", cfg.RateLimit.Window)
	}
	if cfg.RateLimit.Key != "ip" {
		t.Errorf("expected rate_limit.key = ip, got %s", cfg.RateLimit.Key)
	}

	if cfg.Reliability.BreakerFailureThreshold != 5 {
		t.Errorf("expected breaker_failure_threshold = 5, got %d", cfg.Reliability.BreakerFailureThreshold)
	}
	if cfg.Reliability.BreakerCooldown.Duration() != 30*time.Second {
		t.Errorf("expected breaker_cooldown = 30s, got %s", cfg.Reliability.BreakerCooldown)
	}
	if !cfg.Reliability.RetryEnabled {
		t.Errorf("expected retry_enabled = true")
	}
	if cfg.Reliability.RetryMaxAttempts != 3 {
		t.Errorf("expected retry_max_attempts = 3, got %d", cfg.Reliability.RetryMaxAttempts)
	}
	if cfg.Reliability.RetryInitialDelay.Duration() != 100*time.Millisecond {
		t.Errorf("expected retry_initial_delay = 100ms, got %s", cfg.Reliability.RetryInitialDelay)
	}

	if cfg.Queue.Enabled {
		t.Errorf("expected queue.enabled = false")
	}
	if cfg.Queue.MaxInflight != 200 {
		t.Errorf("expected queue.max_inflight = 200, got %d", cfg.Queue.MaxInflight)
	}
	if cfg.Queue.MaxQueueDepth != 1000 {
		t.Errorf("expected queue.max_queue_depth = 1000, got %d", cfg.Queue.MaxQueueDepth)
	}
	if cfg.Queue.CodelTarget.Duration() != 100*time.Millisecond {
		t.Errorf("expected queue.codel_target = 100ms, got %s", cfg.Queue.CodelTarget)
	}
	if cfg.Queue.CodelInterval.Duration() != time.Second {
		t.Errorf("expected queue.codel_interval = 1s, got %s", cfg.Queue.CodelInterval)
	}
	if !reflect.DeepEqual(cfg.Queue.PriorityPaths.High, []string{"/api/payments"}) {
		t.Errorf("expected queue high paths ['/api/payments'], got %v", cfg.Queue.PriorityPaths.High)
	}
	if !reflect.DeepEqual(cfg.Queue.PriorityPaths.Low, []string{"/api/analytics"}) {
		t.Errorf("expected queue low paths ['/api/analytics'], got %v", cfg.Queue.PriorityPaths.Low)
	}

	if cfg.Shedding.Enabled {
		t.Errorf("expected shedding.enabled = false")
	}
	if cfg.Shedding.TargetP99.Duration() != 500*time.Millisecond {
		t.Errorf("expected shedding.target_p99 = 500ms, got %s", cfg.Shedding.TargetP99)
	}
	if cfg.Shedding.MinAdmitRate != 0.05 {
		t.Errorf("expected shedding.min_admit_rate = 0.05, got %f", cfg.Shedding.MinAdmitRate)
	}
	if cfg.Shedding.AdjustInterval.Duration() != time.Second {
		t.Errorf("expected shedding.adjust_interval = 1s, got %s", cfg.Shedding.AdjustInterval)
	}
	if !reflect.DeepEqual(cfg.Shedding.PriorityPaths.High, []string{"/api/payments"}) {
		t.Errorf("expected shedding high paths ['/api/payments'], got %v", cfg.Shedding.PriorityPaths.High)
	}
	if !reflect.DeepEqual(cfg.Shedding.PriorityPaths.Low, []string{"/api/analytics"}) {
		t.Errorf("expected shedding low paths ['/api/analytics'], got %v", cfg.Shedding.PriorityPaths.Low)
	}

	if cfg.Cache.TTL.Duration() != 60*time.Second {
		t.Errorf("expected cache.ttl = 60s, got %s", cfg.Cache.TTL)
	}
	if cfg.Cache.MaxEntries != 10000 {
		t.Errorf("expected cache.max_entries = 10000, got %d", cfg.Cache.MaxEntries)
	}
	if cfg.Cache.MaxBodyBytes != 1048576 {
		t.Errorf("expected cache.max_body_bytes = 1048576, got %d", cfg.Cache.MaxBodyBytes)
	}
	if cfg.Cache.L2Enabled != false {
		t.Errorf("expected cache.l2_enabled = false, got %v", cfg.Cache.L2Enabled)
	}
	if cfg.Cache.L2TTL.Duration() != 5*time.Minute {
		t.Errorf("expected cache.l2_ttl = 5m, got %s", cfg.Cache.L2TTL)
	}
	if cfg.Cache.RedisAddr != "127.0.0.1:6379" {
		t.Errorf("expected cache.redis_addr = '127.0.0.1:6379', got %q", cfg.Cache.RedisAddr)
	}

	if cfg.Observability.Tracing.Enabled {
		t.Errorf("expected observability.tracing.enabled = false")
	}
	if cfg.Observability.Tracing.Exporter != "stdout" {
		t.Errorf("expected observability.tracing.exporter = stdout, got %s", cfg.Observability.Tracing.Exporter)
	}
	if cfg.Observability.Tracing.OTLPEndpoint != "localhost:4318" {
		t.Errorf("expected observability.tracing.otlp_endpoint = localhost:4318, got %s", cfg.Observability.Tracing.OTLPEndpoint)
	}
	if cfg.Observability.Tracing.SampleRatio != 1.0 {
		t.Errorf("expected observability.tracing.sample_ratio = 1.0, got %f", cfg.Observability.Tracing.SampleRatio)
	}

	if cfg.Admin.Enabled {
		t.Errorf("expected admin.enabled = false")
	}
	if cfg.Admin.Addr != "127.0.0.1:9091" {
		t.Errorf("expected admin.addr = '127.0.0.1:9091', got %q", cfg.Admin.Addr)
	}
}

func TestValidate_ExtendedRouteFields(t *testing.T) {
	cfg := parseValidFixture(t)
	cfg.Routes[0].PathRegex = `^/api/v[0-9]+/items`
	cfg.Routes[0].Methods = []string{"GET", "POST"}
	cfg.Routes[0].Headers = map[string]string{"X-Api-Key": "secret"}
	cfg.Routes[0].LB = "least_connections"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid configuration with extended fields, got: %v", err)
	}
}

func TestValidate_RateLimit(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*config.Config)
		wantErrText string
	}{
		{
			name: "valid sliding_window rate_limit",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:     true,
					Limiter:     "sliding_window",
					MaxRequests: 100,
					Window:      config.Duration(time.Minute),
					Key:         "ip",
				}
			},
			wantErrText: "",
		},
		{
			name: "valid token_bucket rate_limit",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:         true,
					Limiter:         "token_bucket",
					MaxRequests:     50,
					RefillPerSecond: 2.5,
					Key:             "api_key",
				}
			},
			wantErrText: "",
		},
		{
			name: "disabled rate_limit ignores missing fields",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled: false,
				}
			},
			wantErrText: "",
		},
		{
			name: "max_requests non-positive",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:     true,
					Limiter:     "sliding_window",
					MaxRequests: 0,
					Window:      config.Duration(time.Minute),
					Key:         "ip",
				}
			},
			wantErrText: "rate_limit.max_requests",
		},
		{
			name: "invalid limiter type",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:     true,
					Limiter:     "leaky_bucket",
					MaxRequests: 10,
					Window:      config.Duration(time.Minute),
					Key:         "ip",
				}
			},
			wantErrText: "rate_limit.limiter",
		},
		{
			name: "invalid key type",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:     true,
					Limiter:     "sliding_window",
					MaxRequests: 10,
					Window:      config.Duration(time.Minute),
					Key:         "session_id",
				}
			},
			wantErrText: "rate_limit.key",
		},
		{
			name: "sliding_window missing or non-positive window",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:     true,
					Limiter:     "sliding_window",
					MaxRequests: 10,
					Window:      0,
					Key:         "ip",
				}
			},
			wantErrText: "rate_limit.window",
		},
		{
			name: "token_bucket missing or non-positive refill rate",
			mutate: func(c *config.Config) {
				c.RateLimit = config.RateLimitConfig{
					Enabled:         true,
					Limiter:         "token_bucket",
					MaxRequests:     10,
					RefillPerSecond: 0,
					Key:             "api_key",
				}
			},
			wantErrText: "rate_limit.refill_per_second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_Reliability(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*config.Config)
		wantErrText string
	}{
		{
			name: "valid reliability config",
			mutate: func(c *config.Config) {
				c.Reliability = config.ReliabilityConfig{
					BreakerFailureThreshold: 5,
					BreakerCooldown:         config.Duration(30 * time.Second),
					RetryEnabled:            true,
					RetryMaxAttempts:        3,
					RetryInitialDelay:       config.Duration(100 * time.Millisecond),
				}
			},
			wantErrText: "",
		},
		{
			name: "breaker_failure_threshold less than 1",
			mutate: func(c *config.Config) {
				c.Reliability = config.ReliabilityConfig{
					BreakerFailureThreshold: 0,
					BreakerCooldown:         config.Duration(30 * time.Second),
				}
			},
			wantErrText: "reliability.breaker_failure_threshold",
		},
		{
			name: "breaker_cooldown non-positive",
			mutate: func(c *config.Config) {
				c.Reliability = config.ReliabilityConfig{
					BreakerFailureThreshold: 5,
					BreakerCooldown:         0,
				}
			},
			wantErrText: "reliability.breaker_cooldown",
		},
		{
			name: "retry_max_attempts less than 1 when retry enabled",
			mutate: func(c *config.Config) {
				c.Reliability = config.ReliabilityConfig{
					BreakerFailureThreshold: 5,
					BreakerCooldown:         config.Duration(30 * time.Second),
					RetryEnabled:            true,
					RetryMaxAttempts:        0,
					RetryInitialDelay:       config.Duration(100 * time.Millisecond),
				}
			},
			wantErrText: "reliability.retry_max_attempts",
		},
		{
			name: "retry_initial_delay non-positive when retry enabled",
			mutate: func(c *config.Config) {
				c.Reliability = config.ReliabilityConfig{
					BreakerFailureThreshold: 5,
					BreakerCooldown:         config.Duration(30 * time.Second),
					RetryEnabled:            true,
					RetryMaxAttempts:        3,
					RetryInitialDelay:       0,
				}
			},
			wantErrText: "reliability.retry_initial_delay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_Queue(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*config.Config)
		wantErrText string
	}{
		{
			name: "valid enabled queue config",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled:       true,
					MaxInflight:   200,
					MaxQueueDepth: 1000,
					CodelTarget:   config.Duration(100 * time.Millisecond),
					CodelInterval: config.Duration(time.Second),
					PriorityPaths: config.PriorityPathsConfig{
						High: []string{"/api/payments"},
						Low:  []string{"/api/analytics"},
					},
				}
			},
			wantErrText: "",
		},
		{
			name: "disabled queue ignores zero fields",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled: false,
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled queue with max_inflight zero",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled:       true,
					MaxInflight:   0,
					MaxQueueDepth: 1000,
					CodelTarget:   config.Duration(100 * time.Millisecond),
					CodelInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "queue.max_inflight",
		},
		{
			name: "enabled queue with max_queue_depth zero",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled:       true,
					MaxInflight:   200,
					MaxQueueDepth: 0,
					CodelTarget:   config.Duration(100 * time.Millisecond),
					CodelInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "queue.max_queue_depth",
		},
		{
			name: "enabled queue with codel_target zero",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled:       true,
					MaxInflight:   200,
					MaxQueueDepth: 1000,
					CodelTarget:   0,
					CodelInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "queue.codel_target",
		},
		{
			name: "enabled queue with codel_interval zero",
			mutate: func(c *config.Config) {
				c.Queue = config.QueueConfig{
					Enabled:       true,
					MaxInflight:   200,
					MaxQueueDepth: 1000,
					CodelTarget:   config.Duration(100 * time.Millisecond),
					CodelInterval: 0,
				}
			},
			wantErrText: "queue.codel_interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_Shedding(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *config.Config)
		wantErrText string
	}{
		{
			name: "disabled shedding with zero values is valid",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled: false,
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled shedding with valid fields",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   0.05,
					AdjustInterval: config.Duration(time.Second),
					PriorityPaths: config.PriorityPathsConfig{
						High: []string{"/api/payments"},
						Low:  []string{"/api/analytics"},
					},
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled shedding with zero target_p99",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      0,
					MinAdmitRate:   0.05,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.target_p99",
		},
		{
			name: "enabled shedding with negative target_p99",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(-10 * time.Millisecond),
					MinAdmitRate:   0.05,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.target_p99",
		},
		{
			name: "enabled shedding with zero min_admit_rate",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   0.0,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.min_admit_rate",
		},
		{
			name: "enabled shedding with negative min_admit_rate",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   -0.1,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.min_admit_rate",
		},
		{
			name: "enabled shedding with min_admit_rate 1.0",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   1.0,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.min_admit_rate",
		},
		{
			name: "enabled shedding with min_admit_rate greater than 1.0",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   1.2,
					AdjustInterval: config.Duration(time.Second),
				}
			},
			wantErrText: "shedding.min_admit_rate",
		},
		{
			name: "enabled shedding with zero adjust_interval",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   0.05,
					AdjustInterval: 0,
				}
			},
			wantErrText: "shedding.adjust_interval",
		},
		{
			name: "enabled shedding with negative adjust_interval",
			mutate: func(c *config.Config) {
				c.Shedding = config.SheddingConfig{
					Enabled:        true,
					TargetP99:      config.Duration(500 * time.Millisecond),
					MinAdmitRate:   0.05,
					AdjustInterval: config.Duration(-time.Second),
				}
			},
			wantErrText: "shedding.adjust_interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_Cache(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *config.Config)
		wantErrText string
	}{
		{
			name: "disabled cache with zero values is valid",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled: false,
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled cache with valid fields",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled cache with zero ttl",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          0,
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
				}
			},
			wantErrText: "cache.ttl",
		},
		{
			name: "enabled cache with negative ttl",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(-time.Minute),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
				}
			},
			wantErrText: "cache.ttl",
		},
		{
			name: "enabled cache with zero max_entries",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   0,
					MaxBodyBytes: 1048576,
				}
			},
			wantErrText: "cache.max_entries",
		},
		{
			name: "enabled cache with negative max_entries",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   -100,
					MaxBodyBytes: 1048576,
				}
			},
			wantErrText: "cache.max_entries",
		},
		{
			name: "enabled cache with zero max_body_bytes",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 0,
				}
			},
			wantErrText: "cache.max_body_bytes",
		},
		{
			name: "enabled cache with negative max_body_bytes",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: -512,
				}
			},
			wantErrText: "cache.max_body_bytes",
		},
		{
			name: "l2_enabled with empty redis_addr",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
					L2Enabled:    true,
					RedisAddr:    "",
					L2TTL:        config.Duration(5 * time.Minute),
				}
			},
			wantErrText: "cache.redis_addr",
		},
		{
			name: "l2_enabled with whitespace-only redis_addr",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
					L2Enabled:    true,
					RedisAddr:    "   ",
					L2TTL:        config.Duration(5 * time.Minute),
				}
			},
			wantErrText: "cache.redis_addr",
		},
		{
			name: "l2_enabled with zero l2_ttl",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
					L2Enabled:    true,
					RedisAddr:    "127.0.0.1:6379",
					L2TTL:        0,
				}
			},
			wantErrText: "cache.l2_ttl",
		},
		{
			name: "l2_enabled with negative l2_ttl",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
					L2Enabled:    true,
					RedisAddr:    "127.0.0.1:6379",
					L2TTL:        config.Duration(-time.Minute),
				}
			},
			wantErrText: "cache.l2_ttl",
		},
		{
			name: "valid l2 configuration",
			mutate: func(c *config.Config) {
				c.Cache = config.CacheConfig{
					Enabled:      true,
					TTL:          config.Duration(60 * time.Second),
					MaxEntries:   10000,
					MaxBodyBytes: 1048576,
					L2Enabled:    true,
					RedisAddr:    "127.0.0.1:6379",
					L2TTL:        config.Duration(5 * time.Minute),
				}
			},
			wantErrText: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_ObservabilityTracing(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *config.Config)
		wantErrText string
	}{
		{
			name: "tracing disabled ignores empty fields",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled: false,
				}
			},
			wantErrText: "",
		},
		{
			name: "invalid exporter",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:     true,
					Exporter:    "zipkin",
					SampleRatio: 1.0,
				}
			},
			wantErrText: "observability.tracing.exporter",
		},
		{
			name: "zero sample_ratio",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:     true,
					Exporter:    "stdout",
					SampleRatio: 0.0,
				}
			},
			wantErrText: "observability.tracing.sample_ratio",
		},
		{
			name: "negative sample_ratio",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:     true,
					Exporter:    "stdout",
					SampleRatio: -0.1,
				}
			},
			wantErrText: "observability.tracing.sample_ratio",
		},
		{
			name: "sample_ratio greater than 1",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:     true,
					Exporter:    "stdout",
					SampleRatio: 1.05,
				}
			},
			wantErrText: "observability.tracing.sample_ratio",
		},
		{
			name: "otlp exporter with empty endpoint",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:      true,
					Exporter:     "otlp",
					OTLPEndpoint: "",
					SampleRatio:  1.0,
				}
			},
			wantErrText: "observability.tracing.otlp_endpoint",
		},
		{
			name: "valid stdout tracing",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:     true,
					Exporter:    "stdout",
					SampleRatio: 0.5,
				}
			},
			wantErrText: "",
		},
		{
			name: "valid otlp tracing",
			mutate: func(c *config.Config) {
				c.Observability.Tracing = config.TracingConfig{
					Enabled:      true,
					Exporter:     "otlp",
					OTLPEndpoint: "localhost:4318",
					SampleRatio:  1.0,
				}
			},
			wantErrText: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func TestValidate_Admin(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *config.Config)
		wantErrText string
	}{
		{
			name: "disabled admin with empty addr is valid",
			mutate: func(c *config.Config) {
				c.Admin = config.AdminConfig{
					Enabled: false,
					Addr:    "",
				}
			},
			wantErrText: "",
		},
		{
			name: "disabled admin with addr is valid",
			mutate: func(c *config.Config) {
				c.Admin = config.AdminConfig{
					Enabled: false,
					Addr:    "127.0.0.1:9091",
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled admin with valid addr is valid",
			mutate: func(c *config.Config) {
				c.Admin = config.AdminConfig{
					Enabled: true,
					Addr:    "127.0.0.1:9091",
				}
			},
			wantErrText: "",
		},
		{
			name: "enabled admin with empty addr",
			mutate: func(c *config.Config) {
				c.Admin = config.AdminConfig{
					Enabled: true,
					Addr:    "",
				}
			},
			wantErrText: "admin.addr",
		},
		{
			name: "enabled admin with invalid addr",
			mutate: func(c *config.Config) {
				c.Admin = config.AdminConfig{
					Enabled: true,
					Addr:    "invalid-addr-missing-port",
				}
			},
			wantErrText: "admin.addr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid configuration, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrText)
			}
			if !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("expected error to contain %q, got: %v", tt.wantErrText, err)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestEffectiveRateLimit(t *testing.T) {
	global := config.RateLimitConfig{
		Enabled:     true,
		Limiter:     "sliding_window",
		MaxRequests: 120,
		Window:      config.Duration(time.Minute),
		Key:         "ip",
	}

	tests := []struct {
		name     string
		override *config.RouteRateLimitConfig
		want     config.RateLimitConfig
	}{
		{
			name:     "no override inherits global",
			override: nil,
			want:     global,
		},
		{
			name:     "empty override inherits global",
			override: &config.RouteRateLimitConfig{},
			want:     global,
		},
		{
			name:     "partial override replaces only set fields",
			override: &config.RouteRateLimitConfig{MaxRequests: ptr(5)},
			want: config.RateLimitConfig{
				Enabled: true, Limiter: "sliding_window", MaxRequests: 5,
				Window: config.Duration(time.Minute), Key: "ip",
			},
		},
		{
			name: "switch limiter and key",
			override: &config.RouteRateLimitConfig{
				Limiter: ptr("token_bucket"), RefillPerSecond: ptr(2.5), Key: ptr("api_key"),
			},
			want: config.RateLimitConfig{
				Enabled: true, Limiter: "token_bucket", MaxRequests: 120,
				Window: config.Duration(time.Minute), RefillPerSecond: 2.5, Key: "api_key",
			},
		},
		{
			name:     "explicit enabled false disables route",
			override: &config.RouteRateLimitConfig{Enabled: ptr(false)},
			want: config.RateLimitConfig{
				Enabled: false, Limiter: "sliding_window", MaxRequests: 120,
				Window: config.Duration(time.Minute), Key: "ip",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{RateLimit: global}
			got := cfg.EffectiveRateLimit(config.RouteConfig{Name: "r", RateLimit: tt.override})
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("EffectiveRateLimit() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestRouteConfig_HasRateLimitOverride(t *testing.T) {
	if (config.RouteConfig{}).HasRateLimitOverride() {
		t.Error("nil override reported as override")
	}
	if (config.RouteConfig{RateLimit: &config.RouteRateLimitConfig{}}).HasRateLimitOverride() {
		t.Error("empty override reported as override")
	}
	if !(config.RouteConfig{RateLimit: &config.RouteRateLimitConfig{MaxRequests: ptr(1)}}).HasRateLimitOverride() {
		t.Error("non-empty override not reported")
	}
}

func TestValidate_RouteRateLimitOverride(t *testing.T) {
	tests := []struct {
		name        string
		global      config.RateLimitConfig
		override    *config.RouteRateLimitConfig
		wantErrText string
	}{
		{
			name: "valid partial override",
			global: config.RateLimitConfig{
				Enabled: true, Limiter: "sliding_window", MaxRequests: 120,
				Window: config.Duration(time.Minute), Key: "ip",
			},
			override: &config.RouteRateLimitConfig{MaxRequests: ptr(5)},
		},
		{
			name: "zero max_requests names the route",
			global: config.RateLimitConfig{
				Enabled: true, Limiter: "sliding_window", MaxRequests: 120,
				Window: config.Duration(time.Minute), Key: "ip",
			},
			override:    &config.RouteRateLimitConfig{MaxRequests: ptr(0)},
			wantErrText: "routes[0].rate_limit.max_requests",
		},
		{
			name: "switch to token_bucket without refill rate",
			global: config.RateLimitConfig{
				Enabled: true, Limiter: "sliding_window", MaxRequests: 120,
				Window: config.Duration(time.Minute), Key: "ip",
			},
			override:    &config.RouteRateLimitConfig{Limiter: ptr("token_bucket")},
			wantErrText: "routes[0].rate_limit.refill_per_second",
		},
		{
			name:        "route enables limiting while global is off and fields are missing",
			global:      config.RateLimitConfig{Enabled: false},
			override:    &config.RouteRateLimitConfig{Enabled: ptr(true), MaxRequests: ptr(5)},
			wantErrText: "routes[0].rate_limit.limiter",
		},
		{
			name:   "route enables limiting while global is off with all fields",
			global: config.RateLimitConfig{Enabled: false},
			override: &config.RouteRateLimitConfig{
				Enabled: ptr(true), Limiter: ptr("sliding_window"), MaxRequests: ptr(5),
				Window: ptr(config.Duration(time.Minute)), Key: ptr("ip"),
			},
		},
		{
			name:        "invalid route override ignored when route disabled",
			global:      config.RateLimitConfig{Enabled: false},
			override:    &config.RouteRateLimitConfig{Enabled: ptr(false), Limiter: ptr("bogus")},
			wantErrText: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseValidFixture(t)
			cfg.RateLimit = tt.global
			cfg.Routes[0].RateLimit = tt.override

			err := cfg.Validate()
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("expected valid config, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErrText)
			}
		})
	}
}

func TestLoad_RouteRateLimitOverrideYAML(t *testing.T) {
	path := t.TempDir() + "/aegora.yaml"
	data := `
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "30s"
  idle_timeout: "60s"
routes:
  - name: "login"
    path_prefix: "/login"
    targets: ["http://127.0.0.1:8081"]
    rate_limit:
      max_requests: 5
      window: "10s"
  - name: "internal"
    path_prefix: "/internal"
    targets: ["http://127.0.0.1:8081"]
    rate_limit:
      enabled: false
  - name: "api"
    path_prefix: "/"
    targets: ["http://127.0.0.1:8081"]
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
rate_limit:
  enabled: true
  limiter: "sliding_window"
  max_requests: 120
  window: "1m"
  key: "ip"
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load() unexpected error: %v", err)
	}

	login := cfg.EffectiveRateLimit(cfg.Routes[0])
	if !login.Enabled || login.MaxRequests != 5 || login.Window != config.Duration(10*time.Second) || login.Key != "ip" {
		t.Fatalf("login effective rate limit = %+v, want enabled, 5 per 10s, key ip", login)
	}
	if internal := cfg.EffectiveRateLimit(cfg.Routes[1]); internal.Enabled {
		t.Fatalf("internal route should be disabled, got %+v", internal)
	}
	if cfg.Routes[2].HasRateLimitOverride() {
		t.Fatal("api route should have no override")
	}
}

func TestLoad_RouteTimeoutYAML(t *testing.T) {
	path := t.TempDir() + "/aegora.yaml"
	data := `
server:
  addr: "127.0.0.1:8080"
  read_timeout: "15s"
  write_timeout: "30s"
  idle_timeout: "60s"
routes:
  - name: "api"
    path_prefix: "/"
    targets: ["http://127.0.0.1:8081"]
    timeout: "2s"
observability:
  log_level: "info"
  metrics_addr: "127.0.0.1:9090"
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load() unexpected error: %v", err)
	}
	if got := cfg.Routes[0].Timeout; got != config.Duration(2*time.Second) {
		t.Fatalf("route timeout = %v, want 2s", got)
	}
}
