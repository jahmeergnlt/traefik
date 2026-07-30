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
// Maps are owned by the Server after switchConfigs; callers must treat
// GetConfig() results as immutable snapshots (deep-copied).
type Configuration struct {
	Routers     map[string]RouterConfig
	Middlewares map[string]MiddlewareConfig
}

// handlerHolder is the only concrete type ever stored in EntryPoint.handler.
// atomic.Value panics if Store is called with a different concrete type than
// the first Store (e.g. http.HandlerFunc at init, then *http.ServeMux on reload).
// Wrapping every handler in handlerHolder keeps the stored type stable while
// still allowing the underlying http.Handler implementation to change.
type handlerHolder struct {
	h http.Handler
}

// EntryPoint represents an entrypoint that can hot-swap its handler tree.
type EntryPoint struct {
	handler atomic.Value // always handlerHolder
}

func (e *EntryPoint) setHandler(h http.Handler) {
	if h == nil {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		})
	}
	e.handler.Store(handlerHolder{h: h})
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v := e.handler.Load()
	if v == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	holder, ok := v.(handlerHolder)
	if !ok || holder.h == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	holder.h.ServeHTTP(w, r)
}

// Server manages the entrypoints and configuration updates.
//
// Configuration updates are applied on a single watcher goroutine. Each update
// rebuilds a complete handler tree from one Configuration snapshot under a
// mutex, then publishes it with a single atomic store. That guarantees:
//  1. no partial router/middleware publish
//  2. request-time handlers always match GetConfig()'s logical snapshot
//  3. concurrent providers cannot interleave mid-rebuild
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
			Routers:     map[string]RouterConfig{},
			Middlewares: map[string]MiddlewareConfig{},
		},
	}
	s.entryPoints["web"].setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			// Drain burst: apply only the latest config so concurrent providers
			// collapse to one atomic publish (last-writer-wins, no partial merge).
			config = s.drainLatest(config)
			s.switchConfigs(config)
		}
	}
}

// drainLatest collapses a burst of pending configs into the newest one.
func (s *Server) drainLatest(config Configuration) Configuration {
	for {
		select {
		case newer := <-s.configurationChan:
			config = newer
		default:
			return config
		}
	}
}

func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

func (s *Server) switchConfigs(config Configuration) {
	// Build the full handler tree BEFORE publishing config or handler so
	// readers never observe a config/handler mismatch.
	mux := http.NewServeMux()

	// Snapshot maps into locals used by closures so handlers never read
	// live Server state after publish.
	routers := cloneRouters(config.Routers)
	middlewares := cloneMiddlewares(config.Middlewares)

	for _, routerCfg := range routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(cfg.ResponseText))
		})

		if cfg.Middleware != "" {
			if mwCfg, ok := middlewares[cfg.Middleware]; ok {
				handler = buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	s.mu.Lock()
	s.currentConfig = Configuration{
		Routers:     routers,
		Middlewares: middlewares,
	}
	// Atomic publish of the complete tree (consistent concrete type).
	s.entryPoints["web"].setHandler(mux)
	s.mu.Unlock()
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

// GetConfig returns a deep copy so callers cannot race with switchConfigs.
func (s *Server) GetConfig() Configuration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Configuration{
		Routers:     cloneRouters(s.currentConfig.Routers),
		Middlewares: cloneMiddlewares(s.currentConfig.Middlewares),
	}
}

func cloneRouters(in map[string]RouterConfig) map[string]RouterConfig {
	out := make(map[string]RouterConfig, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneMiddlewares(in map[string]MiddlewareConfig) map[string]MiddlewareConfig {
	out := make(map[string]MiddlewareConfig, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
