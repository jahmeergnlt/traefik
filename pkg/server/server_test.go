package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestConcurrentConfigurationUpdates verifies that the handler chain remains
// consistent during concurrent provider updates. The response body and
// middleware headers must always agree (both "Old" or both "New").
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

	// Send a continuous stream of HTTP requests to the router
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

	// Run the test for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

// TestConfigurationMerging verifies that partial configs from multiple providers
// are properly merged without losing route or middleware definitions.
func TestConfigurationMerging(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Provider A: sets route1 with mw2, route2 with mw1
	configA := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/a", Middleware: "mw2", ResponseText: "A"},
			"route2": {Path: "/b", Middleware: "mw1", ResponseText: "B"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-MW1", HeaderValue: "val1"},
			"mw2": {HeaderName: "X-MW2", HeaderValue: "val2"},
		},
	}
	s.GetConfigurationChan() <- configA
	time.Sleep(50 * time.Millisecond)

	// Provider B: partial update — only route3
	configB := Configuration{
		Routers: map[string]RouterConfig{
			"route3": {Path: "/c", Middleware: "", ResponseText: "C"},
		},
	}
	s.GetConfigurationChan() <- configB
	time.Sleep(50 * time.Millisecond)

	// Verify merged config contains all routes
	cfg := s.GetConfig()
	if _, ok := cfg.Routers["route1"]; !ok {
		t.Error("route1 missing after merge")
	}
	if _, ok := cfg.Routers["route2"]; !ok {
		t.Error("route2 missing after merge")
	}
	if _, ok := cfg.Routers["route3"]; !ok {
		t.Error("route3 missing after merge")
	}
	if _, ok := cfg.Middlewares["mw1"]; !ok {
		t.Error("mw1 missing after merge")
	}
	if _, ok := cfg.Middlewares["mw2"]; !ok {
		t.Error("mw2 missing after merge")
	}

	// Verify handlers are functional
	ep := s.GetEntryPoint("web")
	for _, tc := range []struct {
		path       string
		wantBody   string
		wantHeader string
		headerKey  string
	}{
		{"/a", "A", "val2", "X-MW2"},
		{"/b", "B", "val1", "X-MW1"},
		{"/c", "C", "", ""},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: expected 200, got %d", tc.path, rec.Code)
		}
		if rec.Body.String() != tc.wantBody {
			t.Errorf("%s: expected body %q, got %q", tc.path, tc.wantBody, rec.Body.String())
		}
		if tc.headerKey != "" {
			if got := rec.Header().Get(tc.headerKey); got != tc.wantHeader {
				t.Errorf("%s: expected header %s=%q, got %q", tc.path, tc.headerKey, tc.wantHeader, got)
			}
		}
	}
}

// TestConfigVersionMonotonic verifies the config version increments on each update.
func TestConfigVersionMonotonic(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	v1 := s.GetConfigVersion()

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"r1": {Path: "/", Middleware: "", ResponseText: "ok"},
		},
	}
	time.Sleep(30 * time.Millisecond)

	v2 := s.GetConfigVersion()
	if v2 <= v1 {
		t.Errorf("version did not increment: %d -> %d", v1, v2)
	}

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"r2": {Path: "/2", Middleware: "", ResponseText: "ok2"},
		},
	}
	time.Sleep(30 * time.Millisecond)

	v3 := s.GetConfigVersion()
	if v3 <= v2 {
		t.Errorf("version did not increment again: %d -> %d", v2, v3)
	}
}

// TestHandlerAtomicSwap verifies that the handler swap is atomic: in-flight
// requests complete with the handler that was active when they started.
func TestHandlerAtomicSwap(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Config A: slow handler
	configA := Configuration{
		Routers: map[string]RouterConfig{
			"slow": {Path: "/slow", Middleware: "", ResponseText: "A"},
		},
	}
	s.GetConfigurationChan() <- configA
	time.Sleep(30 * time.Millisecond)

	ep := s.GetEntryPoint("web")

	var wg sync.WaitGroup
	results := make(chan string, 100)

	// Send concurrent requests while swapping configs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			req := httptest.NewRequest(http.MethodGet, "/slow", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)
			results <- rec.Body.String()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Swap to config B while requests are in flight
	time.Sleep(10 * time.Millisecond)
	configB := Configuration{
		Routers: map[string]RouterConfig{
			"slow": {Path: "/slow", Middleware: "", ResponseText: "B"},
		},
	}
	s.GetConfigurationChan() <- configB

	wg.Wait()
	close(results)

	// Both "A" and "B" are valid — what matters is no corrupted values
	hasA, hasB := false, false
	for r := range results {
		if r == "A" {
			hasA = true
		} else if r == "B" {
			hasB = true
		} else {
			t.Errorf("unexpected response body: %q", r)
		}
	}
	if !hasA || !hasB {
		t.Logf("Only saw one response type (hasA=%v, hasB=%v) — timing dependent", hasA, hasB)
	}
}
