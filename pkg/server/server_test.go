package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentConfigurationUpdates verifies that concurrent configuration
// updates from multiple providers never result in a stale or mismatched
// middleware chain. The response body and middleware header must always be
// consistent — if the body says "New Router", the header must say "New",
// and vice versa.
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
						"mw1": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "Old",
						},
						"mw2": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "New",
						},
					},
				}
				s.GetConfigurationChan() <- configA
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Provider B: updates an unrelated router, but keeps route1 using mw1 (Old)
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
						"mw1": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "Old",
						},
					},
				}
				s.GetConfigurationChan() <- configB
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Send a continuous stream of HTTP requests to the router.
	// Track consistency violations found across all request goroutines.
	var violations int64

	requestWorkers := 4
	for w := 0; w < requestWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := s.GetEntryPoint("web")
			for i := 0; i < 500; i++ {
				req := httptest.NewRequest(http.MethodGet, "/test", nil)
				rec := httptest.NewRecorder()
				ep.ServeHTTP(rec, req)

				if rec.Code == http.StatusOK {
					body := rec.Body.String()
					headerVal := rec.Header().Get("X-Test-Header")

					if body == "New Router" {
						if headerVal != "New" {
							t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
							atomic.AddInt64(&violations, 1)
						}
					} else if body == "Old Router" {
						if headerVal != "Old" {
							t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
							atomic.AddInt64(&violations, 1)
						}
					} else {
						t.Errorf("Unexpected response body: %q", body)
						atomic.AddInt64(&violations, 1)
					}
				}
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Run the test for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	if violations > 0 {
		t.Fatalf("Total consistency violations: %d", violations)
	}
}

// TestConcurrentConfigurationUpdatesWithRaceDetector is a dedicated test that
// stresses the atomic handler swap under high concurrency to surface any
// data races detected by `go test -race`.
func TestConcurrentConfigurationUpdatesWithRaceDetector(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Rapidly push 500 configuration updates from a single goroutine.
	// Each update toggles between two middleware configs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			mw := "mw1"
			resp := "Old Router"
			if i%2 == 0 {
				mw = "mw2"
				resp = "New Router"
			}
			s.GetConfigurationChan() <- Configuration{
				Routers: map[string]RouterConfig{
					"route1": {
						Path:         "/test",
						Middleware:   mw,
						ResponseText: resp,
					},
				},
				Middlewares: map[string]MiddlewareConfig{
					"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
					"mw2": {HeaderName: "X-Test-Header", HeaderValue: "New"},
				},
			}
		}
		close(stopChan)
	}()

	// Concurrently read the entrypoint handler while config updates are happening.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for {
			select {
			case <-stopChan:
				return
			default:
				req := httptest.NewRequest(http.MethodGet, "/test", nil)
				rec := httptest.NewRecorder()
				ep.ServeHTTP(rec, req)
			}
		}
	}()

	wg.Wait()

	// Final config check — the last applied config should be consistent.
	finalConfig := s.GetConfig()
	if finalRouter, ok := finalConfig.Routers["route1"]; ok {
		if finalRouter.Middleware == "mw2" {
			if mw, ok := finalConfig.Middlewares["mw2"]; ok {
				if mw.HeaderValue != "New" {
					t.Errorf("Final config inconsistency: mw2 header = %q, expected 'New'", mw.HeaderValue)
				}
			}
		}
	}
}
