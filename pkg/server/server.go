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
	s.entryPoints["web"].handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	return s
}

func (s *Server) Start(ctx context.Context) {
	go s.watcher(ctx)
}

// GetConfigurationChan returns the channel providers use to publish configs.
//
// OWNERSHIP CONTRACT: sending a Configuration transfers ownership of the
// contained maps to the server. Providers MUST NOT mutate the Configuration —
// or any map it references — after the send. A goroutine may not read from
// and write to the same map without synchronization; any provider that
// reuses a config after sending it is already racing regardless of what the
// consumer does.
//
// The consumer-side copy in the watcher (see copyConfig) is defense-in-depth:
// it guarantees the server never retains provider-owned references, so configs
// that the provider DOES hand off cleanly can never be mutated under the
// server's feet by later code, and GetConfig() can hand out a safe view.
func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

func (s *Server) watcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case config := <-s.configurationChan:
			// Copy under our own ownership before applying. This keeps the
			// server from holding provider-owned map references, so a
			// provider that honors the send contract can never have its
			// config mutated underneath the watcher by the server itself.
			s.switchConfigs(copyConfig(config))
		}
	}
}

// copyConfig copies a Configuration's maps. RouterConfig and MiddlewareConfig
// are value-only structs (strings), so copying the maps is a full copy today.
// If either struct ever gains reference fields, this must become a true deep
// copy.
func copyConfig(config Configuration) Configuration {
	clone := Configuration{
		Routers:     make(map[string]RouterConfig, len(config.Routers)),
		Middlewares: make(map[string]MiddlewareConfig, len(config.Middlewares)),
	}
	for k, v := range config.Routers {
		clone.Routers[k] = v
	}
	for k, v := range config.Middlewares {
		clone.Middlewares[k] = v
	}
	return clone
}

func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

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

	// Swap the active entrypoint handler atomically
	s.entryPoints["web"].handler.Store(mux)
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
	// Return a copy so callers can't mutate the live config maps.
	return copyConfig(s.currentConfig)
}
