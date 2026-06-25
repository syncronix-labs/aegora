// Package proxy provides HTTP reverse proxy functionality
// and request routing primitives for Aegora.
package proxy

// RouteConfig defines the configuration for a backend route.
type RouteConfig struct {
	Name        string
	TargetURL   string
	StripPrefix string
}

// Config contains proxy routing configuration.
type Config struct {
	Routes []RouteConfig
}
