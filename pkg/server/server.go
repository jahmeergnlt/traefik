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

// configVersion wraps a Configuration with a version number for atomic swaps.
type configVersion struct {
	config  Configuration
	version uint64
}

// EntryPoint represents an entrypoint with an atomic handler.
type EntryPoint struct {
	handler atomic.Value // holds http.Handler
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := e.handler.Load()
	if h != nil {
		h.(http.Handler).ServeHTTP(w, r)
	} else {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// Server manages the entrypoints and configuration updates with proper synchronization.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint

	mu                sync.RWMutex
	currentConfig     Configuration
	configVersion     atomic.Uint64 // monotonically increasing version counter

	// switchMu serializes configuration switches to prevent concurrent handler rebuilds.
	switchMu sync.Mutex
}

func NewServer() *Server {
	s := &Server{
		configurationChan: make(chan Configuration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
	}
	// Initialize with a default handler
	s.entryPoints["web"].handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
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
			s.applyConfig(config)
		}
	}
}

// GetConfigurationChan returns a send-only channel for pushing configuration updates.
func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

// mergeConfig merges an incoming partial config into the current config.
// This supports multiple providers sending partial updates simultaneously.
func (s *Server) mergeConfig(newConfig Configuration) Configuration {
	s.mu.RLock()
	current := s.currentConfig
	s.mu.RUnlock()

	merged := Configuration{
		Routers:     make(map[string]RouterConfig),
		Middlewares: make(map[string]MiddlewareConfig),
	}

	// Copy current routers
	for k, v := range current.Routers {
		merged.Routers[k] = v
	}
	// Merge new routers (override existing)
	for k, v := range newConfig.Routers {
		merged.Routers[k] = v
	}

	// Copy current middlewares
	for k, v := range current.Middlewares {
		merged.Middlewares[k] = v
	}
	// Merge new middlewares (override existing)
	for k, v := range newConfig.Middlewares {
		merged.Middlewares[k] = v
	}

	return merged
}

// applyConfig processes a configuration update atomically.
// The entire handler chain is rebuilt under a serializing mutex,
// and the atomic handler swap ensures no in-flight request sees a partial state.
func (s *Server) applyConfig(config Configuration) {
	// Serialize all config applications to prevent interleaved handler builds.
	s.switchMu.Lock()
	defer s.switchMu.Unlock()

	// Merge with current config to support partial updates from multiple providers.
	merged := s.mergeConfig(config)

	// Build the complete handler chain BEFORE updating any visible state.
	mux := s.buildHandlerChain(merged)

	// Increment version counter
	s.configVersion.Add(1)

	// Atomically update the visible configuration and handler.
	// Order: update config first (readers see latest), then swap handler.
	s.mu.Lock()
	s.currentConfig = merged
	s.mu.Unlock()

	// Swap the handler atomically — requests from this point forward use the new chain.
	// In-flight requests using the old handler complete with the old chain.
	s.entryPoints["web"].handler.Store(mux)
}

// buildHandlerChain constructs the full HTTP handler from a configuration.
// This is a pure function with no side effects — safe to call under switchMu.
func (s *Server) buildHandlerChain(config Configuration) http.Handler {
	mux := http.NewServeMux()

	for _, routerCfg := range config.Routers {
		cfg := routerCfg // capture by value for closure

		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the merged configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
				handler = buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	return mux
}

// switchConfigs is kept for backward compatibility but now delegates to applyConfig.
func (s *Server) switchConfigs(config Configuration) {
	s.applyConfig(config)
}

func buildMiddleware(cfg MiddlewareConfig, next http.Handler) http.Handler {
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

// GetConfigVersion returns the current configuration version for monitoring.
func (s *Server) GetConfigVersion() uint64 {
	return s.configVersion.Load()
}
