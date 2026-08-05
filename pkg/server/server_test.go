package server

import (
	"context"
	"fmt"
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

	config1 := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/api", Middleware: "mw1", ResponseText: "Old Response"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Version", HeaderValue: "v1"},
		},
	}

	config2 := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/api", Middleware: "mw2", ResponseText: "New Response"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw2": {HeaderName: "X-Version", HeaderValue: "v2"},
		},
	}

	switchConfigsCh := s.GetConfigurationChan()
	switchConfigsCh <- config1
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	var consistentCount atomic.Int32
	var inconsistentCount atomic.Int32
	var totalRequests atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					req := httptest.NewRequest("GET", "/api", nil)
					rec := httptest.NewRecorder()
					ep := s.GetEntryPoint("web")
					ep.ServeHTTP(rec, req)
					totalRequests.Add(1)
					body := rec.Body.String()
					header := rec.Header().Get("X-Version")
					if (header == "v1" && body == "Old Response") || (header == "v2" && body == "New Response") {
						consistentCount.Add(1)
					} else {
						inconsistentCount.Add(1)
					}
				}
			}
		}()
	}

	for j := 0; j < 5; j++ {
		if j%2 == 0 {
			switchConfigsCh <- config1
		} else {
			switchConfigsCh <- config2
		}
		time.Sleep(40 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	t.Logf("Total: %d, Consistent: %d, Inconsistent: %d", totalRequests.Load(), consistentCount.Load(), inconsistentCount.Load())
	if inconsistentCount.Load() > 0 {
		t.Errorf("FAIL: %d inconsistent responses — stale middleware chain detected!", inconsistentCount.Load())
	}
}

func TestConfigSwapPreservesActiveRequests(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	config := Configuration{
		Routers: map[string]RouterConfig{
			"slow": {Path: "/slow", Middleware: "", ResponseText: "ok"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	s.GetConfigurationChan() <- config
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	var completed atomic.Int32
	ep := s.GetEntryPoint("web")

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/slow", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)
			completed.Add(1)
		}()
	}
	time.Sleep(10 * time.Millisecond)
	s.GetConfigurationChan() <- config
	time.Sleep(500 * time.Millisecond)
	wg.Wait()
	t.Logf("Completed %d requests without panic", completed.Load())
}

func TestNoHandlerReturns404(t *testing.T) {
	ep := &EntryPoint{}
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestSwapHandlerConcurrency(t *testing.T) {
	ep := &EntryPoint{}
	epH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})
	ep.handler = epH

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)
		}()
	}
	for i := 0; i < 10; i++ {
		newH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			fmt.Fprint(w, "new")
		})
		ep.swapHandler(newH)
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
}
