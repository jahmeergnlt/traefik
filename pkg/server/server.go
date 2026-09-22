package server

import (
	"context"
	"net/http"
	"sync/atomic"
)

// RouterConfig represents the configuration for a router.
type RouterConfig struct {
	Path         string
	Middleware   string
	ResponseText string
}

// MiddlewareConfig represents the configuration for a middleware.
type MiddlewareConfig struct {
	HeaderName  string
	HeaderValue string
}

// Configuration represents the dynamic configuration.
type Configuration struct {
	Routers     map[string]RouterConfig
	Middlewares map[string]MiddlewareConfig
}

type runtimeSnapshot struct {
	configuration Configuration
	handler       http.Handler
}

// EntryPoint represents an entrypoint.
type EntryPoint struct {
	snapshot *atomic.Pointer[runtimeSnapshot]
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if e.snapshot == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	snapshot := e.snapshot.Load()
	if snapshot == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	snapshot.handler.ServeHTTP(w, r)
}

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint
	snapshot          atomic.Pointer[runtimeSnapshot]
}

func NewServer() *Server {
	s := &Server{
		configurationChan: make(chan Configuration, 100),
	}
	s.entryPoints = map[string]*EntryPoint{
		"web": {snapshot: &s.snapshot},
	}
	s.snapshot.Store(&runtimeSnapshot{handler: http.NewServeMux()})
	return s
}

func (s *Server) Start(ctx context.Context) {
	go s.watcher(ctx)
}

func (s *Server) watcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case config := <-s.configurationChan:
			s.switchConfigs(config)
		}
	}
}

// GetConfigurationChan returns the queue for complete configuration snapshots.
// Senders must treat submitted maps as immutable: a buffered send can return
// before the watcher has copied the maps. Build fresh maps for each update.
func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

func (s *Server) switchConfigs(config Configuration) {
	config = cloneConfiguration(config)

	mux := http.NewServeMux()

	for _, routerCfg := range config.Routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	// Publish the handler and the configuration as one immutable generation.
	s.snapshot.Store(&runtimeSnapshot{
		configuration: config,
		handler:       mux,
	})
}

func (s *Server) buildMiddleware(cfg MiddlewareConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cfg.HeaderName, cfg.HeaderValue)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) GetEntryPoint(name string) *EntryPoint {
	return s.entryPoints[name]
}

func (s *Server) GetConfig() Configuration {
	snapshot := s.snapshot.Load()
	if snapshot == nil {
		return Configuration{}
	}

	return cloneConfiguration(snapshot.configuration)
}

func cloneConfiguration(config Configuration) Configuration {
	clone := Configuration{}

	if config.Routers != nil {
		clone.Routers = make(map[string]RouterConfig, len(config.Routers))
		for name, router := range config.Routers {
			clone.Routers[name] = router
		}
	}

	if config.Middlewares != nil {
		clone.Middlewares = make(map[string]MiddlewareConfig, len(config.Middlewares))
		for name, middleware := range config.Middlewares {
			clone.Middlewares[name] = middleware
		}
	}

	return clone
}
