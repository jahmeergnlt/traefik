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

// TestConcurrentConfigurationUpdates is the original report's reproduction.
// Before the fix it panicked with
// "sync/atomic: store of inconsistently typed value into Value" on the first
// configuration update, because NewServer stored an http.HandlerFunc and the
// watcher stored an *http.ServeMux into the same atomic.Value.
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

// TestEntryPointStoresConsistentType guards the specific defect that crashed
// the process: NewServer publishes an http.HandlerFunc and switchConfigs
// publishes an *http.ServeMux. An atomic.Value panics when two different
// concrete types are stored, so this asserts the first update does not panic.
func TestEntryPointStoresConsistentType(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// This store used to panic because the initial handler was a
	// http.HandlerFunc and this one is a *http.ServeMux.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("switchConfigs panicked on a configuration update: %v", r)
		}
	}()

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"r": {Path: "/a", ResponseText: "a"},
		},
	}
	time.Sleep(100 * time.Millisecond)

	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/a", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "a" {
		t.Fatalf("after first update got code=%d body=%q, want 200 and %q", rec.Code, rec.Body.String(), "a")
	}
}

// TestRouterAndMiddlewareAlwaysConsistent asserts the property the issue asks
// for: whatever configuration is active, the handler chain that serves a
// request was built from that same configuration. It drives concurrent updates
// where the response body and the middleware header are changed together, so a
// partially applied configuration shows up as a body/header mismatch.
func TestRouterAndMiddlewareAlwaysConsistent(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Two configurations that must always travel together.
	oldCfg := Configuration{
		Routers:     map[string]RouterConfig{"r": {Path: "/x", Middleware: "m", ResponseText: "old"}},
		Middlewares: map[string]MiddlewareConfig{"m": {HeaderName: "X-Gen", HeaderValue: "old"}},
	}
	newCfg := Configuration{
		Routers:     map[string]RouterConfig{"r": {Path: "/x", Middleware: "m", ResponseText: "new"}},
		Middlewares: map[string]MiddlewareConfig{"m": {HeaderName: "X-Gen", HeaderValue: "new"}},
	}

	s.GetConfigurationChan() <- oldCfg
	time.Sleep(50 * time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mismatches int64
	var requests int64

	// Writers: flip between the two configurations as fast as possible.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if id%2 == 0 {
						s.GetConfigurationChan() <- newCfg
					} else {
						s.GetConfigurationChan() <- oldCfg
					}
				}
			}
		}(i)
	}

	// Readers: every response must have a body and header from the same config.
	ep := s.GetEntryPoint("web")
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					rec := httptest.NewRecorder()
					ep.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
					atomic.AddInt64(&requests, 1)
					body := rec.Body.String()
					gen := rec.Header().Get("X-Gen")
					if body != gen {
						atomic.AddInt64(&mismatches, 1)
					}
				}
			}
		}()
	}

	time.Sleep(700 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := atomic.LoadInt64(&requests); n == 0 {
		t.Fatal("no requests were served")
	}
	if n := atomic.LoadInt64(&mismatches); n != 0 {
		t.Fatalf("%d requests observed a partially applied configuration (body and middleware from different configs)", n)
	}
}

// TestGetConfigMatchesServingHandler covers the ordering window.
//
// switchConfigs used to assign currentConfig before building and publishing the
// new handler, so GetConfig could report the new configuration while ServeHTTP
// was still routing through the previous chain.
//
// The guarantee is that the reported configuration and the serving handler are
// published as one value, so they cannot disagree. This asserts it the only way
// it can be asserted without a time-of-check race in the test itself: the
// published pair is loaded once and its two halves must match.
func TestGetConfigMatchesServingHandler(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	oldCfg := Configuration{
		Routers:     map[string]RouterConfig{"r": {Path: "/x", Middleware: "m", ResponseText: "old"}},
		Middlewares: map[string]MiddlewareConfig{"m": {HeaderName: "X-Gen", HeaderValue: "old"}},
	}
	newCfg := Configuration{
		Routers:     map[string]RouterConfig{"r": {Path: "/x", Middleware: "m", ResponseText: "new"}},
		Middlewares: map[string]MiddlewareConfig{"m": {HeaderName: "X-Gen", HeaderValue: "new"}},
	}

	s.GetConfigurationChan() <- oldCfg
	time.Sleep(50 * time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mismatches int64
	var checks int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.GetConfigurationChan() <- newCfg
				s.GetConfigurationChan() <- oldCfg
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := s.GetEntryPoint("web")
			for {
				select {
				case <-stop:
					return
				default:
					// One atomic load of the published pair.
					st := ep.load()
					reported := st.config.Routers["r"].ResponseText

					rec := httptest.NewRecorder()
					st.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
					served := rec.Body.String()

					atomic.AddInt64(&checks, 1)
					if reported != served {
						atomic.AddInt64(&mismatches, 1)
					}
				}
			}
		}()
	}

	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()

	if atomic.LoadInt64(&checks) == 0 {
		t.Fatal("no checks ran")
	}
	if n := atomic.LoadInt64(&mismatches); n != 0 {
		t.Fatalf("published configuration and handler disagreed %d times", n)
	}
}

// TestProviderMutationDoesNotAffectLiveConfig covers map aliasing: the provider
// keeps the maps it sent, and mutating them afterwards must not rewrite live
// routing state.
func TestProviderMutationDoesNotAffectLiveConfig(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	routers := map[string]RouterConfig{
		"r": {Path: "/x", Middleware: "m", ResponseText: "original"},
	}
	middlewares := map[string]MiddlewareConfig{
		"m": {HeaderName: "X-Gen", HeaderValue: "original"},
	}
	s.GetConfigurationChan() <- Configuration{Routers: routers, Middlewares: middlewares}
	time.Sleep(80 * time.Millisecond)

	// The provider reuses its maps, as a caching provider would.
	routers["r"] = RouterConfig{Path: "/x", Middleware: "m", ResponseText: "mutated"}
	middlewares["m"] = MiddlewareConfig{HeaderName: "X-Gen", HeaderValue: "mutated"}
	delete(routers, "r")

	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Body.String() != "original" {
		t.Fatalf("provider mutation changed live routing: body=%q, want %q", rec.Body.String(), "original")
	}
	if got := rec.Header().Get("X-Gen"); got != "original" {
		t.Fatalf("provider mutation changed live middleware: header=%q, want %q", got, "original")
	}
}

// TestGetConfigIsNotAliasedToProviderMaps covers the other half of map
// aliasing: a caller mutating the value returned by GetConfig must not be able
// to change what is serving.
func TestGetConfigIsNotAliasedToProviderMaps(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.GetConfigurationChan() <- Configuration{
		Routers:     map[string]RouterConfig{"r": {Path: "/x", ResponseText: "original"}},
		Middlewares: map[string]MiddlewareConfig{},
	}
	time.Sleep(80 * time.Millisecond)

	got := s.GetConfig()
	got.Routers["r"] = RouterConfig{Path: "/x", ResponseText: "tampered"}

	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Body.String() != "original" {
		t.Fatalf("mutating GetConfig result changed live routing: body=%q", rec.Body.String())
	}
}
