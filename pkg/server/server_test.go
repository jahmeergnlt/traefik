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

func TestFirstConfigSwapDoesNotPanic(t *testing.T) {
	// Regression: atomic.Value panic when init stores HandlerFunc and first
	// reload stores *http.ServeMux.
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.GetConfigurationChan() <- Configuration{
			Routers: map[string]RouterConfig{
				"r1": {Path: "/ok", ResponseText: "ok"},
			},
			Middlewares: map[string]MiddlewareConfig{},
		}
		time.Sleep(30 * time.Millisecond)
		ep := s.GetEntryPoint("web")
		req := httptest.NewRequest(http.MethodGet, "/ok", nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Errorf("got %d %q", rec.Code, rec.Body.String())
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out — possible panic or deadlock on first config swap")
	}
}

func TestConfigAndHandlerStayConsistent(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/test", Middleware: "mw1", ResponseText: "Old Router"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
		},
	}
	time.Sleep(40 * time.Millisecond)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var violations atomic.Int64

	// Provider A -> New
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.GetConfigurationChan() <- Configuration{
					Routers: map[string]RouterConfig{
						"route1": {Path: "/test", Middleware: "mw2", ResponseText: "New Router"},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
						"mw2": {HeaderName: "X-Test-Header", HeaderValue: "New"},
					},
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Provider B -> Old + unrelated
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.GetConfigurationChan() <- Configuration{
					Routers: map[string]RouterConfig{
						"route1":    {Path: "/test", Middleware: "mw1", ResponseText: "Old Router"},
						"unrelated": {Path: "/unrelated", ResponseText: "Unrelated Router"},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
					},
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Hammer requests
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 2000; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				continue
			}
			body := rec.Body.String()
			hdr := rec.Header().Get("X-Test-Header")
			switch body {
			case "New Router":
				if hdr != "New" {
					violations.Add(1)
					t.Errorf("inconsistent: body=%q header=%q", body, hdr)
				}
			case "Old Router":
				if hdr != "Old" {
					violations.Add(1)
					t.Errorf("inconsistent: body=%q header=%q", body, hdr)
				}
			default:
				violations.Add(1)
				t.Errorf("unexpected body %q", body)
			}
		}
	}()

	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("%d consistency violations", violations.Load())
	}
}

func TestGetConfigDeepCopy(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"r": {Path: "/x", ResponseText: "a"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	time.Sleep(30 * time.Millisecond)

	c1 := s.GetConfig()
	c1.Routers["r"] = RouterConfig{Path: "/x", ResponseText: "mutated"}
	c2 := s.GetConfig()
	if c2.Routers["r"].ResponseText != "a" {
		t.Fatalf("GetConfig did not deep-copy; got %q", c2.Routers["r"].ResponseText)
	}
}

func TestConcurrentConfigurationUpdates(t *testing.T) {
	// Keep original test name for compatibility with issue wording.
	TestConfigAndHandlerStayConsistent(t)
}
