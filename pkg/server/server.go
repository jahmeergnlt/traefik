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

// Configuration represents a complete, self-contained dynamic configuration snapshot.
type Configuration struct {
	Routers     map[string]RouterConfig
	Middlewares map[string]MiddlewareConfig
}

// serverState bundles the active handler and its corresponding configuration
// as a single immutable snapshot. This ensures that the handler chain and
// the configuration reported by the API are always consistent.
type serverState struct {
	config  Configuration
	handler http.Handler
}

// EntryPoint represents an entrypoint that serves HTTP requests.
// The active state (handler + config) is swapped atomically.
type EntryPoint struct {
	state atomic.Value // holds *serverState
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := e.state.Load()
	if st != nil {
		st.(*serverState).handler.ServeHTTP(w, r)
	} else {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// currentConfig returns the config that is currently active (matching the handler).
func (e *EntryPoint) currentConfig() Configuration {
	st := e.state.Load()
	if st != nil {
		return st.(*serverState).config
	}
	return Configuration{}
}

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint
	mu                sync.RWMutex
}

func NewServer() *Server {
	ep := &EntryPoint{}
	// Initialize with a consistent default state using *serverState.
	ep.state.Store(&serverState{
		config: Configuration{},
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}),
	})

	s := &Server{
		configurationChan: make(chan Configuration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": ep,
		},
	}
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

// switchConfigs builds a complete, self-consistent serverState from the
// supplied Configuration and swaps it in atomically. The handler and config
// are always swapped together, eliminating the window where they could be
// out of sync.
func (s *Server) switchConfigs(config Configuration) {
	// Build the complete new mux from the configuration snapshot.
	mux := http.NewServeMux()

	for _, routerCfg := range config.Routers {
		cfg := routerCfg // capture by copy
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using this configuration snapshot.
		if cfg.Middleware != "" {
			if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
				handler = buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	// Build the complete state snapshot and swap it atomically.
	// This bundles config + handler together so they never diverge.
	newState := &serverState{
		config:  config,
		handler: mux,
	}

	s.mu.Lock()
	s.entryPoints["web"].state.Store(newState)
	s.mu.Unlock()
}

// buildMiddleware creates a middleware handler that sets the configured header
// before passing the request to the next handler.
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

// GetConfig returns the configuration that is currently active and consistent
// with the handler chain.
func (s *Server) GetConfig() Configuration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ep := s.entryPoints["web"]
	if ep == nil {
		return Configuration{}
	}
	return ep.currentConfig()
}
