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

// cloneConfiguration returns a deep copy of c so that neither the provider
// that sent it nor a caller inspecting it through GetConfig can mutate the
// live state afterwards.
func cloneConfiguration(c Configuration) Configuration {
	out := Configuration{
		Routers:     make(map[string]RouterConfig, len(c.Routers)),
		Middlewares: make(map[string]MiddlewareConfig, len(c.Middlewares)),
	}
	for name, cfg := range c.Routers {
		out.Routers[name] = cfg
	}
	for name, cfg := range c.Middlewares {
		out.Middlewares[name] = cfg
	}
	return out
}

// entryPointState is the immutable active state of an entrypoint: the
// configuration snapshot that produced it together with the fully built
// handler chain (router + middlewares) for that exact snapshot.
//
// Both fields are published behind a single pointer so readers can never
// observe the new configuration together with a stale handler chain, or
// vice versa — the transition is indivisible.
type entryPointState struct {
	config  Configuration
	handler http.Handler
}

// EntryPoint represents an entrypoint.
type EntryPoint struct {
	state atomic.Pointer[entryPointState]
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := e.state.Load()
	if st != nil && st.handler != nil {
		st.handler.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Not Found", http.StatusNotFound)
}

// config returns a copy of the configuration snapshot that produced the
// currently active handler chain.
func (e *EntryPoint) config() Configuration {
	st := e.state.Load()
	if st == nil {
		return Configuration{}
	}
	return cloneConfiguration(st.config)
}

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint

	// mu serializes configuration merges/rebuilds so that updates applied
	// through switchConfigs can never interleave and publish a partially
	// applied state.
	mu sync.Mutex
}

func NewServer() *Server {
	s := &Server{
		configurationChan: make(chan Configuration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
	}
	// Initialize with a default handler
	s.entryPoints["web"].state.Store(&entryPointState{
		config: Configuration{},
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}),
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
	s.mu.Lock()
	defer s.mu.Unlock()

	// Take private ownership of the incoming configuration first: providers
	// may reuse or mutate their maps after sending, and the merged state must
	// never be corrupted by that.
	config = cloneConfiguration(config)

	// Rebuild the entire entrypoint handler — router plus the resolved
	// middleware chain — as a single immutable unit before anything is
	// published. Until the swap below, in-flight requests keep using the
	// previous complete chain.
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

	// Publish the configuration and its handler chain together in one atomic
	// store. There is no window in which the API/Dashboard can report the new
	// configuration while requests still execute a stale middleware chain.
	s.entryPoints["web"].state.Store(&entryPointState{
		config:  config,
		handler: mux,
	})
}

func (s *Server) buildMiddleware(cfg MiddlewareConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cfg.HeaderName, cfg.HeaderValue)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) GetEntryPoint(name string) *EntryPoint {
	return s.entryPoints[name]
}

// GetConfig returns a copy of the configuration snapshot that produced the
// currently active handler chain. The returned maps are private to the
// caller.
func (s *Server) GetConfig() Configuration {
	return s.entryPoints["web"].config()
}
