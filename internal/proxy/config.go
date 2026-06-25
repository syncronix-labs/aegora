package proxy

type RouteConfig struct {
	Name        string
	TargetURL   string
	StripPrefix string
}

type Config struct {
	Routes []RouteConfig
}
