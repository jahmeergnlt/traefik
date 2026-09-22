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

// ProviderConfig holds the configuration from a single provider.
type ProviderConfig struct {
	Routers     map[string]RouterConfig
	Middlewares map[string]MiddlewareConfig
}

// EntryPoint represents an entrypoint.
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

// Server manages the entrypoints and configuration updates.
type Server struct {
	providerConfigs map[string]*ProviderConfig
	providerMu      sync.RWMutex
	entryPoints     map[string]*EntryPoint
	mu              sync.RWMutex
	currentConfig   Configuration
	configVersion   uint64
}

func NewServer() *Server {
	s := &Server{
		providerConfigs: make(map[string]*ProviderConfig),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
	}
	// Initialize with a default handler (http.ServeMux implements http.Handler)
	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
	s.entryPoints["web"].handler.Store(defaultMux)
	return s
}

// RegisterProvider registers a new configuration provider and returns a channel for sending updates.
func (s *Server) RegisterProvider(providerID string) chan<- Configuration {
	s.providerMu.Lock()
	defer s.providerMu.Unlock()

	if existing, exists := s.providerConfigs[providerID]; exists {
		// Return a channel that forwards to the existing provider's update logic
		ch := make(chan Configuration, 100)
		go func() {
			for config := range ch {
				s.updateProviderConfig(providerID, config)
			}
		}()
		_ = existing // suppress unused warning
		return ch
	}

	ch := make(chan Configuration, 100)
	s.providerConfigs[providerID] = &ProviderConfig{
		Routers:     make(map[string]RouterConfig),
		Middlewares: make(map[string]MiddlewareConfig),
	}

	go func() {
		for config := range ch {
			s.updateProviderConfig(providerID, config)
		}
	}()

	return ch
}

func (s *Server) updateProviderConfig(providerID string, config Configuration) {
	s.providerMu.Lock()
	providerCfg, exists := s.providerConfigs[providerID]
	if !exists {
		s.providerMu.Unlock()
		return
	}
	providerCfg.Routers = config.Routers
	providerCfg.Middlewares = config.Middlewares
	s.providerMu.Unlock()

	s.mergeAndRebuild()
}

func (s *Server) mergeAndRebuild() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.providerMu.RLock()
	merged := Configuration{
		Routers:     make(map[string]RouterConfig),
		Middlewares: make(map[string]MiddlewareConfig),
	}
	for _, providerCfg := range s.providerConfigs {
		for name, router := range providerCfg.Routers {
			merged.Routers[name] = router
		}
		for name, mw := range providerCfg.Middlewares {
			merged.Middlewares[name] = mw
		}
	}
	s.providerMu.RUnlock()

	s.currentConfig = merged
	atomic.AddUint64(&s.configVersion, 1)

	// Rebuild the entrypoint handler atomically as a single, immutable unit.
	mux := http.NewServeMux()

	for _, routerCfg := range merged.Routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := merged.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	// Swap the active entrypoint handler atomically
	s.entryPoints["web"].handler.Store(mux)
}

func (s *Server) buildMiddleware(cfg MiddlewareConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cfg.HeaderName, cfg.HeaderValue)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Start(ctx context.Context) {
	// No watcher needed with provider-based model
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

func (s *Server) GetConfigVersion() uint64 {
	return atomic.LoadUint64(&s.configVersion)
}