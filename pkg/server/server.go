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

// clone returns a deep copy of the configuration.
//
// The maps handed to switchConfigs belong to whichever provider produced them.
// Keeping references to them would mean a provider that reuses or mutates a map
// after sending it silently rewrites live routing state, and a caller that
// mutates the value returned by GetConfig would do the same. The clone is what
// makes the published configuration immutable in practice, not just by
// convention.
func (c Configuration) clone() Configuration {
	out := Configuration{
		Routers:     make(map[string]RouterConfig, len(c.Routers)),
		Middlewares: make(map[string]MiddlewareConfig, len(c.Middlewares)),
	}
	for k, v := range c.Routers {
		out.Routers[k] = v
	}
	for k, v := range c.Middlewares {
		out.Middlewares[k] = v
	}
	return out
}

// entryPointState is the immutable pair that an entrypoint serves from.
//
// Config and handler travel together because they are only meaningful
// together: GetConfig reports what the API and Dashboard show, and handler is
// what actually serves the request. Publishing them as one value means a
// reader can never see the new router configuration while requests still go
// through the previous middleware chain, or the reverse.
//
// The field is also a single concrete type, which is what keeps repeated stores
// into the atomic.Value legal: an atomic.Value panics when a second, different
// concrete type is stored into it, and previously the watcher stored an
// *http.ServeMux while NewServer had stored an http.HandlerFunc.
type entryPointState struct {
	config  Configuration
	handler http.Handler
}

// EntryPoint represents an entrypoint.
type EntryPoint struct {
	state atomic.Value // holds entryPointState
}

func notFoundHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := e.load()
	st.handler.ServeHTTP(w, r)
}

func (e *EntryPoint) load() entryPointState {
	loaded := e.state.Load()
	if loaded == nil {
		return entryPointState{handler: notFoundHandler()}
	}
	st, ok := loaded.(entryPointState)
	if !ok || st.handler == nil {
		return entryPointState{handler: notFoundHandler()}
	}
	return st
}

// store publishes a configuration and its handler as one indivisible value.
func (e *EntryPoint) store(config Configuration, handler http.Handler) {
	if handler == nil {
		handler = notFoundHandler()
	}
	e.state.Store(entryPointState{config: config, handler: handler})
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
	// Initialize with a default handler and an empty configuration.
	s.entryPoints["web"].store(Configuration{}, notFoundHandler())
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

// switchConfigs builds the complete handler set for config and publishes it,
// together with the configuration itself, as one immutable unit.
//
// Nothing becomes observable until the final store: the routing tree and every
// middleware chain it references are built from the same snapshot first, so a
// request can never observe a router from one configuration and a middleware
// from another, and GetConfig can never report a configuration that is not yet
// serving. The configuration is cloned on the way in, so a provider that
// mutates a map after sending it cannot corrupt live state.
func (s *Server) switchConfigs(config Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := config.clone()

	// Build the new routing tree completely before publishing anything.
	mux := http.NewServeMux()

	for _, routerCfg := range snapshot.Routers {
		cfg := routerCfg
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the same snapshot.
		if cfg.Middleware != "" {
			if mwCfg, ok := snapshot.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	s.currentConfig = snapshot

	// Single publish point: configuration and handler become visible together.
	s.entryPoints["web"].store(snapshot, mux)
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

// GetConfig returns the configuration that is currently serving requests.
//
// The returned value is read from the same immutable pair that ServeHTTP uses,
// and it is a clone, so a caller cannot mutate live state through it.
func (s *Server) GetConfig() Configuration {
	if ep := s.GetEntryPoint("web"); ep != nil {
		return ep.load().config.clone()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentConfig.clone()
}
