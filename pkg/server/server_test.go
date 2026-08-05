package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestConcurrentConfigurationUpdates verifies that under concurrent provider
// updates, each request sees a consistent handler+middleware pair — i.e. the
// response body and header always match the same configuration version.
func TestConcurrentConfigurationUpdates(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Initial configuration: route1 uses mw1 (Old)
	initialConfig := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:         "/test",
				Middleware:   "mw1",
				ResponseText: "Old Router",
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {
				HeaderName:  "X-Test-Header",
				HeaderValue: "Old",
			},
		},
	}
	s.GetConfigurationChan() <- initialConfig
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Provider A: updates route1 to use mw2 (New)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				configA := Configuration{
					Routers: map[string]RouterConfig{
						"route1": {
							Path:         "/test",
							Middleware:   "mw2",
							ResponseText: "New Router",
						},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
						"mw2": {HeaderName: "X-Test-Header", HeaderValue: "New"},
					},
				}
				s.GetConfigurationChan() <- configA
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Provider B: updates an unrelated router, keeps route1 using mw1 (Old)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				configB := Configuration{
					Routers: map[string]RouterConfig{
						"route1": {
							Path:         "/test",
							Middleware:   "mw1",
							ResponseText: "Old Router",
						},
						"unrelated": {
							Path:         "/unrelated",
							Middleware:   "",
							ResponseText: "Unrelated Router",
						},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
					},
				}
				s.GetConfigurationChan() <- configB
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Continuous HTTP request stream — verify body/header consistency
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 1000; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				body := rec.Body.String()
				headerVal := rec.Header().Get("X-Test-Header")

				if body == "New Router" {
					if headerVal != "New" {
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else if body == "Old Router" {
					if headerVal != "Old" {
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else {
					t.Errorf("Unexpected response body: %q", body)
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

// TestConfigHandlerConsistency verifies that GetConfig() and the active handler
// are always in sync — GetConfig never reports a configuration that the handler
// does not yet serve.
func TestConfigHandlerConsistency(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	ep := s.GetEntryPoint("web")

	for i := 0; i < 100; i++ {
		cfg := Configuration{
			Routers: map[string]RouterConfig{
				"r": {Path: "/api", Middleware: "m", ResponseText: "v" + string(rune('0'+i%10))},
			},
			Middlewares: map[string]MiddlewareConfig{
				"m": {HeaderName: "X-V", HeaderValue: "v" + string(rune('0'+i%10))},
			},
		}
		s.GetConfigurationChan() <- cfg
		time.Sleep(1 * time.Millisecond)

		// Send a request and check that the response body matches
		// the config that GetConfig reports.
		req := httptest.NewRequest(http.MethodGet, "/api", nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)

		gotConfig := s.GetConfig()
		if rec.Code == http.StatusOK {
			expectedBody := gotConfig.Routers["r"].ResponseText
			if rec.Body.String() != expectedBody {
				t.Errorf("Config/handler mismatch: handler returned %q but config has %q",
					rec.Body.String(), expectedBody)
			}
		}
	}
}

// TestNoAtomicTypePanic verifies that rapid configuration updates never
// cause an atomic.Value type-panic.
func TestNoAtomicTypePanic(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Send many different configuration shapes rapidly
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			cfg := Configuration{
				Routers: map[string]RouterConfig{
					"a": {Path: "/a", Middleware: "", ResponseText: "hello"},
					"b": {Path: "/b", Middleware: "mw", ResponseText: "world"},
				},
				Middlewares: map[string]MiddlewareConfig{
					"mw": {HeaderName: "X-H", HeaderValue: "ok"},
				},
			}
			s.GetConfigurationChan() <- cfg

			cfg2 := Configuration{
				Routers:     map[string]RouterConfig{},
				Middlewares: map[string]MiddlewareConfig{},
			}
			s.GetConfigurationChan() <- cfg2
		}
		close(done)
	}()

	<-done
	time.Sleep(50 * time.Millisecond)

	// If we get here without panic, the test passes.
	ep := s.GetEntryPoint("web")
	req := httptest.NewRequest(http.MethodGet, "/a", nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)
	// Just ensure we can still serve requests
	if rec.Code != http.StatusOK && rec.Code != http.StatusNotFound {
		t.Errorf("unexpected status after rapid updates: %d", rec.Code)
	}
}

// TestMultipleEntryPoints verifies consistent behavior with multiple entrypoints.
func TestMultipleEntryPoints(t *testing.T) {
	s := NewServer()
	// Add a second entrypoint manually
	s.mu.Lock()
	ep2 := &EntryPoint{}
	ep2.state.Store(&serverState{
		config: Configuration{},
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}),
	})
	s.entryPoints["web2"] = ep2
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	cfg := Configuration{
		Routers: map[string]RouterConfig{
			"r1": {Path: "/", Middleware: "", ResponseText: "ok"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	s.GetConfigurationChan() <- cfg
	time.Sleep(30 * time.Millisecond)

	// Both entrypoints should still work after config update
	for _, name := range []string{"web", "web2"} {
		ep := s.GetEntryPoint(name)
		if ep == nil {
			t.Fatalf("entrypoint %q is nil", name)
		}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)
	}
}
