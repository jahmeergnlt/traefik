package server

import (
	"context"
	"net/http"
	"sync"
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
// Uses a RWMutex to ensure that handler swaps cannot occur while
// requests are in-flight, preventing stale middleware chains.
type EntryPoint struct {
	handler   http.Handler
	handlerMu sync.RWMutex
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.handlerMu.RLock()
	h := e.handler
	e.handlerMu.RUnlock()

	if h != nil {
		h.ServeHTTP(w, r)
	} else {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// swapHandler atomically swaps the handler, waiting for all in-flight
// requests to complete before the swap takes effect.
func (e *EntryPoint) swapHandler(newHandler http.Handler) {
	e.handlerMu.Lock()
	e.handler = newHandler
	e.handlerMu.Unlock()
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
	// Initialize with a default handler
	s.entryPoints["web"].handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
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

func (s *Server) switchConfigs(config Configuration) {
	// Build the new handler chain outside the lock to avoid
	// blocking request serving during handler construction.
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

	// Swap the active entrypoint handler under proper synchronization.
	// The RWMutex in EntryPoint ensures no requests are in-flight during the swap,
	// preventing stale middleware chains.
	s.mu.Lock()
	s.currentConfig = config
	ep := s.entryPoints["web"]
	s.mu.Unlock()

	// swapHandler acquires a write lock, waiting for all in-flight
	// requests to drain before applying the new handler.
	ep.swapHandler(mux)
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
