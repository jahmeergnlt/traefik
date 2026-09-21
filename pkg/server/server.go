package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	// Assume other necessary imports exist, e.g., for configuration merging and router building
)

// ConfigMessage represents a configuration update coming from a provider.
type ConfigMessage struct {
	// The concrete fields depend on the original implementation.
	// For illustration we keep a generic payload.
	Payload interface{}
}

// Server holds the HTTP server and the dynamic configuration handling logic.
type Server struct {
	// configurationChan receives configuration updates from providers.
	configurationChan chan ConfigMessage

	// routerHolder stores the active router atomically.
	// The stored value is of type *Router (or whatever the concrete router type is).
	routerHolder atomic.Value // *Router

	// lock serialises configuration merges and router rebuilds.
	// It prevents concurrent updates from leaving the system in a partially‑updated state.
	updateLock sync.Mutex

	// httpServer is the underlying net/http server.
	httpServer *http.Server

	// ctx is used to control the server lifecycle.
	ctx context.Context
}

// NewServer creates a Server instance ready to receive configuration updates.
func NewServer(ctx context.Context, addr string) *Server {
	s := &Server{
		configurationChan: make(chan ConfigMessage, 32), // buffered to avoid blocking providers
		ctx:                ctx,
	}
	// Initialise the atomic router with an empty placeholder to avoid nil derefs.
	s.routerHolder.Store(newRouter()) // newRouter creates a minimal, safe router instance.

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: http.HandlerFunc(s.handleRequest),
	}
	return s
}

// Start launches the HTTP server and the configuration processing loop.
func (s *Server) Start() error {
	// Run the configuration consumer in its own goroutine.
	go s.consumeConfigurations()

	// Start listening for HTTP traffic.
	go func() {
		<-s.ctx.Done()
		_ = s.httpServer.Shutdown(context.Background())
	}()

	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// consumeConfigurations reads from configurationChan and applies updates atomically.
func (s *Server) consumeConfigurations() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case cfg := <-s.configurationChan:
			s.applyConfiguration(cfg)
		}
	}
}

// applyConfiguration merges the incoming configuration with the current state
// and atomically swaps the router used for request handling.
func (s *Server) applyConfiguration(cfg ConfigMessage) {
	// Ensure only one configuration update is processed at a time.
	s.updateLock.Lock()
	defer s.updateLock.Unlock()

	// 1️⃣ Merge the new configuration with the existing one.
	// The actual merge logic depends on the original codebase.
	mergedCfg, err := mergeConfiguration(cfg)
	if err != nil {
		// Log the error; for brevity we just ignore it here.
		return
	}

	// 2️⃣ Build a new router based on the merged configuration.
	newR := buildRouterFromConfig(mergedCfg)

	// 3️⃣ Atomically replace the router used by the request handler.
	s.routerHolder.Store(newR)
}

// handleRequest is the entry point for every incoming HTTP request.
// It always fetches the latest router atomically, guaranteeing that the
// middleware chain matches the currently exposed configuration.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Load the most recent router.
	router := s.routerHolder.Load().(*Router)

	// Delegate the request to the router. The router implementation is
	// assumed to implement http.Handler.
	router.ServeHTTP(w, r)
}

// SubmitConfig allows external providers to push configuration updates.
func (s *Server) SubmitConfig(cfg ConfigMessage) {
	select {
	case s.configurationChan <- cfg:
		// queued successfully
	default:
		// In case the channel buffer is full we drop the update to avoid blocking.
		// Real implementations could use a more sophisticated back‑pressure strategy.
	}
}

/* -------------------------------------------------------------------------- */
/* Helper functions – placeholders for the original implementation details.   */
/* -------------------------------------------------------------------------- */

// Router is a placeholder for the actual router type used by Traefik.
// It must implement http.Handler.
type Router struct {
	// internal fields omitted
}

// newRouter creates a minimal, safe router instance used during Server initialisation.
func newRouter() *Router {
	// Return a router that simply returns 404 for any request.
	return &Router{}
}

// ServeHTTP satisfies http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Placeholder implementation – the real router would dispatch to middlewares/handlers.
	http.NotFound(w, req)
}

// mergeConfiguration merges a ConfigMessage into the current configuration.
// The real implementation would be far more complex.
func mergeConfiguration(cfg ConfigMessage) (interface{}, error) {
	// Stub: simply return the payload.
	return cfg.Payload, nil
}

// buildRouterFromConfig builds a new Router based on the merged configuration.
// The real implementation would construct the full middleware chain.
func buildRouterFromConfig(cfg interface{}) *Router {
	// Stub: return a fresh router instance.
	return newRouter()
}
