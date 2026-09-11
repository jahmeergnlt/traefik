package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
)

// ProviderConfiguration represents configuration from a single provider.
type ProviderConfiguration struct {
	ProviderName string
	Configuration
}

// providerConfigs stores the last configuration from each provider.
// This enables proper merging of configurations from multiple providers.
type providerConfigs map[string]Configuration


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

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan ProviderConfiguration
	entryPoints       map[string]*EntryPoint
	mu                sync.RWMutex
	providerConfigs   providerConfigs
	currentConfig     Configuration
}


func (s *Server) Start(ctx context.Context) {
	s := &Server{
		configurationChan: make(chan ProviderConfiguration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
		providerConfigs: make(providerConfigs),
	}
	// Initialize with a default handler
	s.entryPoints["web"].handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	s.mu.Lock()
		select {
		case <-ctx.Done():
			return
		case providerConfig := <-s.configurationChan:
			s.applyProviderConfig(providerConfig)
		}
	}
}

func (s *Server) GetConfigurationChan() chan<- ProviderConfiguration {
	return s.configurationChan
}

// applyProviderConfig stores the provider's configuration and rebuilds the merged config.
func (s *Server) applyProviderConfig(pc ProviderConfiguration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store this provider's configuration
	s.providerConfigs[pc.ProviderName] = pc.Configuration

	// Merge all provider configurations into a single configuration
	merged := s.mergeConfigs()

	// Apply the merged configuration
	s.switchConfigsLocked(merged)
}

// mergeConfigs merges configurations from all providers.
// Later providers (alphabetically) take precedence for conflicting keys.
func (s *Server) mergeConfigs() Configuration {
	merged := Configuration{
		Routers:     make(map[string]RouterConfig),
		Middlewares: make(map[string]MiddlewareConfig),
	}

	for _, cfg := range s.providerConfigs {
		for name, router := range cfg.Routers {
			merged.Routers[name] = router
		}
		for name, mw := range cfg.Middlewares {
			merged.Middlewares[name] = mw
		}
	}

	return merged
}

// switchConfigsLocked rebuilds the handler chain. Caller must hold s.mu.
func (s *Server) switchConfigsLocked(config Configuration) {
	s.currentConfig = config

	// Rebuild the entrypoint handler atomically as a single, immutable unit.
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
	return s.currentConfig
}
