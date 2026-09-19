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
//
// handler holds exactly one concrete type, handlerHolder, so that repeated
// stores into the atomic.Value never mix types. An atomic.Value panics if a
// second, different concrete type is stored into it, and previously the
// watcher stored a bare *http.ServeMux while NewServer stored an
// http.HandlerFunc, which crashed the process on the first configuration
// update.
type EntryPoint struct {
	handler atomic.Value // holds handlerHolder
}

// handlerHolder wraps a handler so every store has the same concrete type.
type handlerHolder struct {
	handler http.Handler
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	holder := e.handler.Load()
	if holder == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	h, ok := holder.(handlerHolder)
	if !ok || h.handler == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	h.handler.ServeHTTP(w, r)
}

// store publishes a handler atomically. A nil handler is replaced by a
// not-found handler so the entrypoint never panics on a request.
func (e *EntryPoint) store(h http.Handler) {
	if h == nil {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		})
	}
	e.handler.Store(handlerHolder{handler: h})
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
	s.entryPoints["web"].store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	return s
}

func (s *Server) Start(ctx context.Context) {
	go s.watcher(ctx)
}

// watcher consumes configuration updates one at a time and applies them
// serially. Serializing here is what makes the transition from one
// configuration to the next atomic with respect to other updates: a
// configuration is never merged into a half-applied predecessor.
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

// switchConfigs builds the complete handler set for config and publishes it as
// one immutable unit. The router tree and every middleware chain it references
// are constructed from the same config value before anything becomes visible,
// so a request can never observe a router from one configuration and a
// middleware from another.
func (s *Server) switchConfigs(config Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentConfig = config

	// Build the new routing tree completely before publishing it.
	mux := http.NewServeMux()

	for _, routerCfg := range config.Routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	// Swap the active entrypoint handler atomically. This is the only point at
	// which the new configuration becomes observable, and it is a single store.
	s.entryPoints["web"].store(mux)
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
