package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
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
// The active handler is held in an atomic.Pointer rather than an atomic.Value.
// atomic.Value requires every Store to use the same concrete type, so storing
// an http.HandlerFunc during setup and an *http.ServeMux on the first update
// panics at runtime. Keying the pointer to the http.Handler interface makes the
// invariant a compile-time property instead of a convention callers must follow.
type EntryPoint struct {
	handler atomic.Pointer[http.Handler]
}

// setHandler atomically publishes h as the entrypoint's active handler.
// It is the only way the handler is written, so no call site can reintroduce
// a type mismatch.
func (e *EntryPoint) setHandler(h http.Handler) {
	e.handler.Store(&h)
}

func (e *EntryPoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p := e.handler.Load(); p != nil && *p != nil {
		(*p).ServeHTTP(w, r)
		return
	}
	http.Error(w, "Not Found", http.StatusNotFound)
}

// Server manages the entrypoints and configuration updates.
type Server struct {
	configurationChan chan Configuration
	entryPoints       map[string]*EntryPoint
	mu                sync.RWMutex
	currentConfig     Configuration
	lastErrors        []string
}

func NewServer() *Server {
	s := &Server{
		configurationChan: make(chan Configuration, 100),
		entryPoints: map[string]*EntryPoint{
			"web": {},
		},
	}
	// Initialize with a default handler
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
			s.applyConfig(config)
		}
	}
}

// applyConfig runs a single configuration switch, isolating the watcher loop
// from any panic it might raise. Without this, one malformed configuration
// kills the goroutine and every later update is silently dropped, leaving the
// handler chain stale until the process restarts.
func (s *Server) applyConfig(config Configuration) {
	defer func() {
		if r := recover(); r != nil {
			s.recordError(fmt.Errorf("panic applying configuration: %v", r))
		}
	}()

	s.switchConfigs(config)
}

func (s *Server) GetConfigurationChan() chan<- Configuration {
	return s.configurationChan
}

func (s *Server) switchConfigs(config Configuration) {
	// Build the complete chain before taking the lock. Construction reads only
	// the incoming snapshot, so holding the lock across it would block readers
	// for no reason.
	handler, problems := s.buildHandlerChain(config)

	// Publish the configuration and the handler built from it in one critical
	// section. GetConfig therefore never reports a configuration that the live
	// handler chain does not already implement, which is the consistency
	// guarantee this issue is about.
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentConfig = config
	s.lastErrors = problems
	s.entryPoints["web"].setHandler(handler)
}

// buildHandlerChain builds the complete handler chain for a configuration.
//
// It reads no mutable server state, so it is safe to call before acquiring the
// lock. Routers are visited in sorted key order so a given configuration always
// produces an identical chain; map iteration order is randomised, which would
// otherwise let conflict resolution differ between two builds of the same input.
//
// Empty and duplicate paths are reported and skipped rather than registered,
// because http.ServeMux panics on both.
func (s *Server) buildHandlerChain(config Configuration) (http.Handler, []string) {
	mux := http.NewServeMux()

	names := make([]string, 0, len(config.Routers))
	for name := range config.Routers {
		names = append(names, name)
	}
	sort.Strings(names)

	var problems []string
	claimed := make(map[string]string, len(names))

	for _, name := range names {
		cfg := config.Routers[name]

		if cfg.Path == "" {
			problems = append(problems, fmt.Sprintf("router %q: empty path, skipped", name))
			continue
		}
		if owner, dup := claimed[cfg.Path]; dup {
			problems = append(problems, fmt.Sprintf("router %q: path %q already claimed by router %q, skipped", name, cfg.Path, owner))
			continue
		}
		claimed[cfg.Path] = name

		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(cfg.ResponseText))
		})

		// Wrap with middleware if configured, using the configuration snapshot
		if cfg.Middleware != "" {
			if mwCfg, ok := config.Middlewares[cfg.Middleware]; ok {
				handler = s.buildMiddleware(mwCfg, handler)
			} else {
				problems = append(problems, fmt.Sprintf("router %q: middleware %q not defined", name, cfg.Middleware))
			}
		}

		mux.Handle(cfg.Path, handler)
	}

	return mux, problems
}

// recordError notes a failure that prevented a configuration from being applied.
func (s *Server) recordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErrors = []string{err.Error()}
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

// GetConfigErrors returns the problems found while applying the most recent
// configuration, such as routers skipped for conflicting paths. An empty result
// means the configuration was applied in full.
func (s *Server) GetConfigErrors() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.lastErrors) == 0 {
		return nil
	}
	return append([]string(nil), s.lastErrors...)
}
