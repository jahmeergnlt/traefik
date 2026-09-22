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

func TestConcurrentConfigurationUpdates(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Register two providers
	providerA := s.RegisterProvider("provider-a")
	providerB := s.RegisterProvider("provider-b")

	// Initial configuration from both providers
	initialConfigA := Configuration{
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
	initialConfigB := Configuration{
		Routers: map[string]RouterConfig{
			"unrelated": {
				Path:         "/unrelated",
				Middleware:   "",
				ResponseText: "Unrelated Router",
			},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	providerA <- initialConfigA
	providerB <- initialConfigB
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	consistencyViolations := atomic.Int32{}

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
				providerA <- configA
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
				providerB <- configB
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Send a continuous stream of HTTP requests to the router
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 2000; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				body := rec.Body.String()
				headerVal := rec.Header().Get("X-Test-Header")

				if body == "New Router" {
					if headerVal != "New" {
						consistencyViolations.Add(1)
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else if body == "Old Router" {
					if headerVal != "Old" {
						consistencyViolations.Add(1)
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else {
					consistencyViolations.Add(1)
					t.Errorf("Unexpected response body: %q", body)
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Run the test for a short duration
	time.Sleep(1 * time.Second)
	close(stopChan)
	wg.Wait()

	if consistencyViolations.Load() > 0 {
		t.Errorf("Test failed with %d consistency violations", consistencyViolations.Load())
	}
}

func TestProviderConfigMerge(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	providerA := s.RegisterProvider("provider-a")
	providerB := s.RegisterProvider("provider-b")

	// Provider A provides route1 with mw1
	providerA <- Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/a", Middleware: "mw1", ResponseText: "A"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Source", HeaderValue: "A"},
		},
	}

	// Provider B provides route2 with mw2
	providerB <- Configuration{
		Routers: map[string]RouterConfig{
			"route2": {Path: "/b", Middleware: "mw2", ResponseText: "B"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw2": {HeaderName: "X-Source", HeaderValue: "B"},
		},
	}

	time.Sleep(50 * time.Millisecond)

	config := s.GetConfig()
	if len(config.Routers) != 2 {
		t.Errorf("Expected 2 routers, got %d", len(config.Routers))
	}
	if _, ok := config.Routers["route1"]; !ok {
		t.Error("Expected route1 from provider A")
	}
	if _, ok := config.Routers["route2"]; !ok {
		t.Error("Expected route2 from provider B")
	}
	if len(config.Middlewares) != 2 {
		t.Errorf("Expected 2 middlewares, got %d", len(config.Middlewares))
	}

	// Verify handler works for both routes
	ep := s.GetEntryPoint("web")

	reqA := httptest.NewRequest(http.MethodGet, "/a", nil)
	recA := httptest.NewRecorder()
	ep.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK || recA.Body.String() != "A" || recA.Header().Get("X-Source") != "A" {
		t.Errorf("Route A failed: code=%d body=%s header=%s", recA.Code, recA.Body.String(), recA.Header().Get("X-Source"))
	}

	reqB := httptest.NewRequest(http.MethodGet, "/b", nil)
	recB := httptest.NewRecorder()
	ep.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK || recB.Body.String() != "B" || recB.Header().Get("X-Source") != "B" {
		t.Errorf("Route B failed: code=%d body=%s header=%s", recB.Code, recB.Body.String(), recB.Header().Get("X-Source"))
	}
}

func TestConfigVersionIncrements(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	providerA := s.RegisterProvider("provider-a")

	initialVersion := s.GetConfigVersion()

	providerA <- Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/test", Middleware: "mw1", ResponseText: "Test"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Test", HeaderValue: "Value"},
		},
	}

	time.Sleep(50 * time.Millisecond)

	newVersion := s.GetConfigVersion()
	if newVersion <= initialVersion {
		t.Errorf("Config version should increment, was %d now %d", initialVersion, newVersion)
	}
}