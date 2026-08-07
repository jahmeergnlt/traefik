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

	// Initial configuration: route1 uses mw1 (Old)
	initialConfig := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:       "/test",
				Middleware: "mw1",
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
							Path:       "/test",
							Middleware: "mw2",
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
							Path:       "/test",
							Middleware: "mw1",
							ResponseText: "Old Router",
						},
						"unrelated": {
							Path:       "/unrelated",
							Middleware: "",
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

	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

func TestConfigAndHandlerConsistency(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	config := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:       "/test",
				Middleware: "mw1",
				ResponseText: "Test",
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {
				HeaderName:  "X-Test",
				HeaderValue: "Value1",
			},
		},
	}
	s.GetConfigurationChan() <- config
	time.Sleep(50 * time.Millisecond)

	// Verify config and handler are consistent
	cfg := s.GetConfig()
	if _, ok := cfg.Routers["route1"]; !ok {
		t.Fatal("expected route1 in config")
	}

	ep := s.GetEntryPoint("web")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "Test" {
		t.Errorf("expected 'Test', got %q", rec.Body.String())
	}
	if rec.Header().Get("X-Test") != "Value1" {
		t.Errorf("expected 'Value1', got %q", rec.Header().Get("X-Test"))
	}
}

func TestConcurrentConfigAndHandlerSync(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writer: continuously sends new configs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			val := "Old"
			if i%2 == 0 {
				val = "New"
			}
			cfg := Configuration{
				Routers: map[string]RouterConfig{
					"route1": {
						Path:       "/test",
						Middleware: "mw1",
						ResponseText: val + " Router",
					},
				},
				Middlewares: map[string]MiddlewareConfig{
					"mw1": {
						HeaderName:  "X-Test-Header",
						HeaderValue: val,
					},
				},
			}
			s.GetConfigurationChan() <- cfg
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Reader: continuously checks config and handler consistency
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 500 && !stop.Load(); i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				body := rec.Body.String()
				header := rec.Header().Get("X-Test-Header")
				// Body and header must always be consistent
				if body == "New Router" && header != "New" {
					t.Errorf("Consistency violation: body=%q header=%q", body, header)
				}
				if body == "Old Router" && header != "Old" {
					t.Errorf("Consistency violation: body=%q header=%q", body, header)
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
