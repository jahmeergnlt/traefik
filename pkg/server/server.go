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

// Clone creates a deep copy of the configuration to avoid concurrent map read/write issues.
func (c Configuration) Clone() Configuration {
	clone := Configuration{
		Routers:     make(map[string]RouterConfig, len(c.Routers)),
		Middlewares: make(map[string]MiddlewareConfig, len(c.Middlewares)),
	}
	for k, v := range c.Routers {
		clone.Routers[k] = v
	}
	for k, v := range c.Middlewares {
		clone.Middlewares[k] = v
	}
	return clone
}

// handlerWrapper guarantees consistent concrete type for atomic.Value stores.
type handlerWrapper struct {
	handler http.Handler
}

// EntryPoint represents an entrypoint.
type EntryPoint struct {
	handler atomic.Value // holds handlerWrapper
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := e.handler.Load()
	if h != nil {
		if wrapper, ok := h.(handlerWrapper); ok && wrapper.handler != nil {
			wrapper.handler.ServeHTTP(w, r)
			return
		}
	}
	http.Error(w, "Not Found", http.StatusNotFound)
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
		currentConfig: Configuration{
			Routers:     make(map[string]RouterConfig),
			Middlewares: make(map[string]MiddlewareConfig),
		},
	}
	// Initialize with a default handler wrapped in handlerWrapper
	defaultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
	s.entryPoints["web"].handler.Store(handlerWrapper{handler: defaultHandler})
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
	// Create an isolated snapshot copy to guarantee immutability during rebuild
	snapshot := config.Clone()

	// Rebuild the entrypoint handler atomically as a single, immutable unit.
	mux := http.NewServeMux()

	for _, routerCfg := range snapshot.Routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := snapshot.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	s.mu.Lock()
	s.currentConfig = snapshot
	ep := s.entryPoints["web"]
	s.mu.Unlock()

	if ep != nil {
		// Swap the active entrypoint handler atomically using the same concrete type
		ep.handler.Store(handlerWrapper{handler: mux})
	}
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
	return s.currentConfig.Clone()
}
