package server

import (
	"context"
	"sync"
	"sync/atomic"
)

// Config represents a simplified configuration payload coming from a provider.
// In the real Traefik code‑base this would be far more complex, but for the
// purpose of fixing the race condition we only need a deterministic structure
// that can be merged and used to rebuild the router.
type Config struct {
	// Routers maps a router name to its configuration.
	Routers map[string]RouterConfig
	// Middlewares maps a middleware name to its configuration.
	Middlewares map[string]MiddlewareConfig
}

// RouterConfig is a placeholder for router‑specific settings.
type RouterConfig struct {
	Rule        string
	Middlewares []string
}

// MiddlewareConfig is a placeholder for middleware‑specific settings.
type MiddlewareConfig struct {
	Type string
}

// Router is the runtime representation of the routing tree together with the
// resolved middleware chain.  It is deliberately simple – the real implementation
// contains many more fields – but it is sufficient to demonstrate that the
// handler chain is always in sync with the configuration.
type Router struct {
	// Resolved middlewares for each router (router name → middleware chain).
	Chains map[string][]MiddlewareConfig
}

// Server holds the core runtime loop that receives configuration updates from
// providers and atomically swaps the active router.
type Server struct {
	// configurationChan receives configuration updates from providers.
	configurationChan chan *Config

	// mu protects the merge/rebuild process so that only one goroutine can
	// rebuild the router at a time.  This eliminates the race where a stale
	// router could be swapped in while another update is still being processed.
	mu sync.Mutex

	// router holds the currently active router.  It is stored in an atomic.Value
	// to allow lock‑free reads from request handling code.
	router atomic.Value // *Router
}

// NewServer creates a Server with a buffered channel of the given size.
func NewServer(chanSize int) *Server {
	return &Server{
		configurationChan: make(chan *Config, chanSize),
	}
}

// Run starts the main configuration loop.  It should be called in a goroutine.
func (s *Server) Run(ctx context.Context) {
	for {
		select {
		case cfg := <-s.configurationChan:
			s.handleConfig(cfg)
		case <-ctx.Done():
			return
		}
	}
}

// UpdateConfig pushes a new configuration onto the internal channel.  This is
// the entry‑point used by providers.
func (s *Server) UpdateConfig(cfg *Config) {
	s.configurationChan <- cfg
}

// GetRouter returns the currently active router (or nil if none has been built yet).
func (s *Server) GetRouter() *Router {
	if v := s.router.Load(); v != nil {
		return v.(*Router)
	}
	return nil
}

// handleConfig merges the incoming configuration with the existing one and
// atomically swaps the router.  The whole operation is guarded by a mutex to
// guarantee that concurrent updates are processed sequentially.
func (s *Server) handleConfig(cfg *Config) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// In a real implementation we would merge the new config with the previous
	// one.  For this simplified example we just rebuild the router from scratch.
	newRouter := buildRouter(cfg)

	// Store the new router atomically – any request handling code that reads the
	// router will see either the old complete router or the new complete router,
	// never a partially built one.
	s.router.Store(newRouter)
}

// buildRouter creates a Router from a Config.  It resolves the middleware
// chain for each router so that the runtime representation is fully self‑contained.
func buildRouter(cfg *Config) *Router {
	if cfg == nil {
		return &Router{Chains: map[string][]MiddlewareConfig{}}
	}

	chains := make(map[string][]MiddlewareConfig, len(cfg.Routers))
	for name, r := range cfg.Routers {
		var chain []MiddlewareConfig
		for _, mwName := range r.Middlewares {
			if mw, ok := cfg.Middlewares[mwName]; ok {
				chain = append(chain, mw)
			}
		}
		chains[name] = chain
	}
	return &Router{Chains: chains}
}
