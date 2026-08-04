package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

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
	s.UpdateConfiguration(initialConfig)
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
				s.UpdateConfiguration(configA)
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
				s.UpdateConfiguration(configB)
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

func TestConfigurationSnapshotIsolatedFromCallerMutation(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:         "/test",
				Middleware:   "mw1",
				ResponseText: "Original Router",
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {
				HeaderName:  "X-Test-Header",
				HeaderValue: "Original",
			},
		},
	}

	s.Start(ctx)

	s.UpdateConfiguration(config)
	config.Routers["route1"] = RouterConfig{
		Path:         "/test",
		Middleware:   "mw1",
		ResponseText: "Mutated Router",
	}
	config.Middlewares["mw1"] = MiddlewareConfig{
		HeaderName:  "X-Test-Header",
		HeaderValue: "Mutated",
	}

	time.Sleep(50 * time.Millisecond)

	ep := s.GetEntryPoint("web")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	if body := rec.Body.String(); body != "Original Router" {
		t.Fatalf("expected original router response, got %q", body)
	}

	if headerVal := rec.Header().Get("X-Test-Header"); headerVal != "Original" {
		t.Fatalf("expected original middleware header, got %q", headerVal)
	}
}
