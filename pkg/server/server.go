package server

import (
	"context"
	"net/http"
	"sync"
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

// EntryPoint represents an entrypoint.
// handler always stores *http.Handler (pointer to interface) so that every
// atomic.Value.Store call uses the same concrete type, preventing
// sync/atomic "inconsistently typed value" panics.
type EntryPoint struct {
	handler atomic.Value // holds *http.Handler
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hp := e.handler.Load()
	if hp != nil {
		h := *hp.(*http.Handler)
		h.ServeHTTP(w, r)
	} else {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint
	mu                sync.RWMutex
	currentConfig     Configuration
}

func NewServer() *Server {
	s := &Server{
		configurationChan: make(chan Configuration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
	}
	// Initialize with a default handler — stored as *http.Handler so every
	// subsequent atomic.Value.Store uses the same concrete type (*http.Handler).
	defaultHandler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	s.entryPoints["web"].handler.Store(&defaultHandler)
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

func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

// switchConfigs processes a configuration update atomically. The lock ensures
// that concurrent configuration updates from multiple providers are serialized —
// no partial state can be observed by request handlers or by GetConfig.
//
// The handler chain (router + middleware) is built entirely within the lock
// from a single immutable configuration snapshot, then swapped into the
// entrypoint's atomic.Value as one unit. This guarantees that the active
// HTTP handler chain always reflects the current router configuration.
func (s *Server) switchConfigs(config Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentConfig = config

	// Rebuild the entrypoint handler atomically as a single, immutable unit.
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

	// Swap the active entrypoint handler atomically.
	// Wrap in *http.Handler to match the type stored in NewServer,
	// preventing sync/atomic "inconsistently typed value" panics.
	handler := http.Handler(mux)
	s.entryPoints["web"].handler.Store(&handler)
}

func (s *Server) buildMiddleware(cfg MiddlewareConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cfg.HeaderName, cfg.HeaderValue)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) GetEntryPoint(name string) *EntryPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entryPoints[name]
}

func (s *Server) GetConfig() Configuration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentConfig
}
