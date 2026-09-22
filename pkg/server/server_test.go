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

func TestUnconfiguredEntryPoint(t *testing.T) {
	for name, ep := range map[string]*EntryPoint{
		"zero value": {},
		"new server": NewServer().GetEntryPoint("web"),
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("got status %d, want 404", rec.Code)
			}
		})
	}
}

func TestConfigurationSnapshotIsImmutable(t *testing.T) {
	s := NewServer()
	config := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:         "/test",
				Middleware:   "mw1",
				ResponseText: "original body",
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {
				HeaderName:  "X-Test-Header",
				HeaderValue: "original header",
			},
		},
	}

	s.switchConfigs(config)

	config.Routers["route1"] = RouterConfig{
		Path:         "/test",
		Middleware:   "mw1",
		ResponseText: "mutated body",
	}
	config.Middlewares["mw1"] = MiddlewareConfig{
		HeaderName:  "X-Test-Header",
		HeaderValue: "mutated header",
	}

	returned := s.GetConfig()
	returned.Routers["route1"] = RouterConfig{ResponseText: "mutated return value"}
	delete(returned.Middlewares, "mw1")

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); body != "original body" {
		t.Fatalf("unexpected response body: got %q, want %q", body, "original body")
	}
	if header := rec.Header().Get("X-Test-Header"); header != "original header" {
		t.Fatalf("unexpected response header: got %q, want %q", header, "original header")
	}

	stored := s.GetConfig()
	if body := stored.Routers["route1"].ResponseText; body != "original body" {
		t.Fatalf("stored router was mutated: got %q, want %q", body, "original body")
	}
	if header := stored.Middlewares["mw1"].HeaderValue; header != "original header" {
		t.Fatalf("stored middleware was mutated: got %q, want %q", header, "original header")
	}
}

func TestPublishedSnapshotKeepsConfigurationAndHandlerTogether(t *testing.T) {
	s := NewServer()
	configs := []Configuration{
		{
			Routers: map[string]RouterConfig{
				"route1": {Path: "/test", Middleware: "mw", ResponseText: "first"},
			},
			Middlewares: map[string]MiddlewareConfig{
				"mw": {HeaderName: "X-Generation", HeaderValue: "first"},
			},
		},
		{
			Routers: map[string]RouterConfig{
				"route1": {Path: "/test", Middleware: "mw", ResponseText: "second"},
			},
			Middlewares: map[string]MiddlewareConfig{
				"mw": {HeaderName: "X-Generation", HeaderValue: "second"},
			},
		},
	}

	var writers sync.WaitGroup
	for i := 0; i < 4; i++ {
		writers.Add(1)
		go func(offset int) {
			defer writers.Done()
			for j := 0; j < 500; j++ {
				s.switchConfigs(configs[(offset+j)%len(configs)])
			}
		}(i)
	}

	for i := 0; i < 2000; i++ {
		snapshot := s.snapshot.Load()
		router, ok := snapshot.configuration.Routers["route1"]
		if !ok {
			continue
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		snapshot.handler.ServeHTTP(rec, req)

		if body := rec.Body.String(); body != router.ResponseText {
			t.Fatalf("handler and configuration came from different generations: body=%q config=%q", body, router.ResponseText)
		}
		middleware := snapshot.configuration.Middlewares[router.Middleware]
		if header := rec.Header().Get(middleware.HeaderName); header != middleware.HeaderValue {
			t.Fatalf("middleware and configuration came from different generations: header=%q config=%q", header, middleware.HeaderValue)
		}
	}

	writers.Wait()
}
