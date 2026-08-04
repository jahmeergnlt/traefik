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

type entryPointHandler struct {
	handler http.Handler
}

// EntryPoint represents an entrypoint.
type EntryPoint struct {
	handler atomic.Value // holds *entryPointHandler
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := e.handler.Load()
	if h != nil {
		h.(*entryPointHandler).handler.ServeHTTP(w, r)
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
	s.entryPoints["web"].handler.Store(&entryPointHandler{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})})
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
			config = copyConfiguration(config)
			for {
				select {
				case nextConfig := <-s.configurationChan:
					config = copyConfiguration(nextConfig)
				default:
					s.switchConfigs(config)
					goto nextIteration
				}
			}
		}
	nextIteration:
	}
}

func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

func (s *Server) UpdateConfiguration(config Configuration) {
	s.configurationChan <- copyConfiguration(config)
}

func (s *Server) switchConfigs(config Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := copyConfiguration(config)
	s.currentConfig = snapshot

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

	// Swap the active entrypoint handler atomically
	s.entryPoints["web"].handler.Store(&entryPointHandler{handler: mux})
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
	return copyConfiguration(s.currentConfig)
}

func copyConfiguration(config Configuration) Configuration {
	return Configuration{
		Routers:     copyRouterConfigs(config.Routers),
		Middlewares: copyMiddlewareConfigs(config.Middlewares),
	}
}

func copyRouterConfigs(source map[string]RouterConfig) map[string]RouterConfig {
	if source == nil {
		return nil
	}

	cloned := make(map[string]RouterConfig, len(source))
	for name, routerCfg := range source {
		cloned[name] = routerCfg
	}

	return cloned
}

func copyMiddlewareConfigs(source map[string]MiddlewareConfig) map[string]MiddlewareConfig {
	if source == nil {
		return nil
	}

	cloned := make(map[string]MiddlewareConfig, len(source))
	for name, middlewareCfg := range source {
		cloned[name] = middlewareCfg
	}

	return cloned
}
