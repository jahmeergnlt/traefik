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
	violations := make(chan string, 1000)
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
						violations <- "Consistency violation: response body is New Router but header is " + headerVal
					}
				} else if body == "Old Router" {
					if headerVal != "Old" {
						violations <- "Consistency violation: response body is Old Router but header is " + headerVal
					}
				} else {
					violations <- "Unexpected response body: " + body
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Run the test for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
	close(violations)

	violationCount := 0
	for v := range violations {
		t.Error(v)
		violationCount++
	}
	if violationCount == 0 {
		t.Log("No consistency violations detected")
	}
}

func TestConfigSwapAtomicity(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Send config A
	configA := Configuration{
		Routers: map[string]RouterConfig{
			"r1": {Path: "/a", ResponseText: "ConfigA"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	s.GetConfigurationChan() <- configA
	time.Sleep(50 * time.Millisecond)

	// Rapidly alternate between two configs
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				s.GetConfigurationChan() <- configA
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	configB := Configuration{
		Routers: map[string]RouterConfig{
			"r1": {Path: "/a", ResponseText: "ConfigB"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				s.GetConfigurationChan() <- configB
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Verify that GetConfig always returns a consistent snapshot
	// and that the response body is always one of the valid values
	invalidResponses := make(chan string, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 500; i++ {
			req := httptest.NewRequest(http.MethodGet, "/a", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				body := rec.Body.String()
				if body != "ConfigA" && body != "ConfigB" {
					invalidResponses <- "Invalid response: " + body
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			cfg := s.GetConfig()
			// Verify that the config returned is a valid snapshot
			// (either ConfigA or ConfigB, never partially applied)
			if _, ok := cfg.Routers["r1"]; !ok {
				invalidResponses <- "Config missing expected router"
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stopChan)
	wg.Wait()
	close(invalidResponses)

	for v := range invalidResponses {
		t.Error(v)
	}
}

func TestRaceDetector(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Rapid concurrent config updates to trigger any data races
	configs := []Configuration{
		{
			Routers:     map[string]RouterConfig{"r1": {Path: "/a", Middleware: "m1", ResponseText: "A"}},
			Middlewares: map[string]MiddlewareConfig{"m1": {HeaderName: "X-A", HeaderValue: "a"}},
		},
		{
			Routers:     map[string]RouterConfig{"r1": {Path: "/a", Middleware: "m2", ResponseText: "B"}},
			Middlewares: map[string]MiddlewareConfig{"m2": {HeaderName: "X-B", HeaderValue: "b"}},
		},
		{
			Routers:     map[string]RouterConfig{"r1": {Path: "/a", ResponseText: "C"}},
			Middlewares: map[string]MiddlewareConfig{},
		},
	}

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.GetConfigurationChan() <- configs[idx%len(configs)]
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Readers
	ep := s.GetEntryPoint("web")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				req := httptest.NewRequest(http.MethodGet, "/a", nil)
				rec := httptest.NewRecorder()
				ep.ServeHTTP(rec, req)
				_ = s.GetConfig()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
	cancel()
}
