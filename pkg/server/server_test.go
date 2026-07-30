package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// --- helpers ---------------------------------------------------------------

// newTestServer returns a started server whose watcher stops with the test.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.Start(ctx)
	return s
}

// waitFor polls cond until it holds or the deadline passes. Configurations are
// applied asynchronously by the watcher, so tests wait for an update to land
// instead of assuming a fixed delay.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, desc)
}

// get issues a request against an entrypoint and returns the recorded response.
func get(ep *EntryPoint, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)
	return rec
}

// routerConfig builds a single-router configuration whose middleware sets
// X-Test-Header to headerValue.
func routerConfig(path, body, headerValue string) Configuration {
	return Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: path, Middleware: "mw", ResponseText: body},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw": {HeaderName: "X-Test-Header", HeaderValue: headerValue},
		},
	}
}

// --- tests -----------------------------------------------------------------

// TestDefaultHandler404BeforeConfig checks the entrypoint is usable before any
// configuration has been received.
func TestDefaultHandler404BeforeConfig(t *testing.T) {
	s := NewServer()

	rec := get(s.GetEntryPoint("web"), "/anything")
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 before any config, got %d", rec.Code)
	}
}

// TestHandlerSwapAppliesAfterFirstConfig is the regression guard for the bug
// this PR fixes. The entrypoint handler is seeded with an http.HandlerFunc and
// later replaced with an *http.ServeMux; storing both in an atomic.Value panics
// on the type change, so the very first update never reaches the handler chain
// and every request keeps hitting the default 404.
func TestHandlerSwapAppliesAfterFirstConfig(t *testing.T) {
	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	s.GetConfigurationChan() <- routerConfig("/hello", "hello world", "v1")

	waitFor(t, 2*time.Second, "first configuration to be applied", func() bool {
		return get(ep, "/hello").Code == http.StatusOK
	})

	rec := get(ep, "/hello")
	if got := rec.Body.String(); got != "hello world" {
		t.Errorf("expected body %q, got %q", "hello world", got)
	}
	if got := rec.Header().Get("X-Test-Header"); got != "v1" {
		t.Errorf("expected header %q, got %q", "v1", got)
	}
}

// TestNoPartialMiddlewareApplication drives the exact symptom described in the
// issue: a response must never carry one configuration's body alongside another
// configuration's middleware header.
func TestNoPartialMiddlewareApplication(t *testing.T) {
	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	s.GetConfigurationChan() <- routerConfig("/test", "Old Router", "Old")
	waitFor(t, 2*time.Second, "initial configuration", func() bool {
		return get(ep, "/test").Code == http.StatusOK
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Two providers flipping the same route between two complete snapshots.
	for _, snapshot := range []Configuration{
		routerConfig("/test", "Old Router", "Old"),
		routerConfig("/test", "New Router", "New"),
	} {
		cfg := snapshot
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case s.GetConfigurationChan() <- cfg:
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			rec := get(ep, "/test")
			if rec.Code != http.StatusOK {
				continue
			}
			body := rec.Body.String()
			header := rec.Header().Get("X-Test-Header")

			switch body {
			case "Old Router":
				if header != "Old" {
					t.Errorf("torn state: body %q served with header %q", body, header)
				}
			case "New Router":
				if header != "New" {
					t.Errorf("torn state: body %q served with header %q", body, header)
				}
			default:
				t.Errorf("unexpected body %q", body)
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConcurrentProviderUpdates widens the existing acceptance test to several
// providers pushing distinct snapshots at once, which is the multi-provider
// scenario the issue describes.
func TestConcurrentProviderUpdates(t *testing.T) {
	const providers = 4

	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	bodyFor := func(i int) string { return fmt.Sprintf("provider-%d", i) }
	headerFor := func(i int) string { return fmt.Sprintf("header-%d", i) }

	s.GetConfigurationChan() <- routerConfig("/test", bodyFor(0), headerFor(0))
	waitFor(t, 2*time.Second, "initial configuration", func() bool {
		return get(ep, "/test").Code == http.StatusOK
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < providers; i++ {
		cfg := routerConfig("/test", bodyFor(i), headerFor(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case s.GetConfigurationChan() <- cfg:
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Body and header must always originate from the same provider's snapshot.
	valid := make(map[string]string, providers)
	for i := 0; i < providers; i++ {
		valid[bodyFor(i)] = headerFor(i)
	}

	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				rec := get(ep, "/test")
				if rec.Code != http.StatusOK {
					continue
				}
				body := rec.Body.String()
				want, ok := valid[body]
				if !ok {
					t.Errorf("unexpected body %q", body)
					continue
				}
				if got := rec.Header().Get("X-Test-Header"); got != want {
					t.Errorf("torn state: body %q served with header %q, want %q", body, got, want)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConfigAndHandlerAgreeAfterChurn checks that once updates stop, the
// configuration reported by the API and the chain actually serving traffic
// describe the same thing. The issue reports these drifting apart and staying
// apart until the process restarts.
func TestConfigAndHandlerAgreeAfterChurn(t *testing.T) {
	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	for i := 0; i < 50; i++ {
		s.GetConfigurationChan() <- routerConfig("/test", fmt.Sprintf("body-%d", i), fmt.Sprintf("header-%d", i))
	}

	final := routerConfig("/test", "final-body", "final-header")
	s.GetConfigurationChan() <- final

	waitFor(t, 2*time.Second, "final configuration to settle", func() bool {
		return get(ep, "/test").Body.String() == "final-body"
	})

	cfg := s.GetConfig()
	router, ok := cfg.Routers["route1"]
	if !ok {
		t.Fatal("route1 missing from reported configuration")
	}

	rec := get(ep, "/test")
	if rec.Body.String() != router.ResponseText {
		t.Errorf("API reports body %q but handler serves %q", router.ResponseText, rec.Body.String())
	}
	if got := rec.Header().Get("X-Test-Header"); got != cfg.Middlewares[router.Middleware].HeaderValue {
		t.Errorf("API reports header %q but handler serves %q", cfg.Middlewares[router.Middleware].HeaderValue, got)
	}
}

// TestRouteRemovedOnConfigReplace pins the replace semantics of a config
// update: a router absent from the new snapshot must stop serving. Merging
// snapshots instead would leave withdrawn routes alive indefinitely.
func TestRouteRemovedOnConfigReplace(t *testing.T) {
	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"keep": {Path: "/keep", ResponseText: "keep"},
			"drop": {Path: "/drop", ResponseText: "drop"},
		},
	}
	waitFor(t, 2*time.Second, "both routes to be served", func() bool {
		return get(ep, "/keep").Code == http.StatusOK && get(ep, "/drop").Code == http.StatusOK
	})

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"keep": {Path: "/keep", ResponseText: "keep"},
		},
	}
	waitFor(t, 2*time.Second, "withdrawn route to stop serving", func() bool {
		return get(ep, "/drop").Code == http.StatusNotFound
	})

	if rec := get(ep, "/keep"); rec.Code != http.StatusOK {
		t.Errorf("retained route stopped serving: got %d", rec.Code)
	}
}

// TestWatcherSurvivesMalformedConfig checks that a configuration which cannot
// be fully applied neither crashes the watcher nor stops later updates. An
// unrecovered panic here is what makes a stale chain persist until restart.
func TestWatcherSurvivesMalformedConfig(t *testing.T) {
	s := newTestServer(t)
	ep := s.GetEntryPoint("web")

	// Two routers claim the same path and a third has none at all. http.ServeMux
	// panics on either, so both must be reported and skipped instead.
	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"aaa":     {Path: "/shared", ResponseText: "from aaa"},
			"bbb":     {Path: "/shared", ResponseText: "from bbb"},
			"nopath":  {Path: "", ResponseText: "unreachable"},
			"healthy": {Path: "/healthy", ResponseText: "ok"},
		},
	}
	waitFor(t, 2*time.Second, "malformed configuration to be applied", func() bool {
		return get(ep, "/healthy").Code == http.StatusOK
	})

	// The first router in sorted order wins the contested path, deterministically.
	if got := get(ep, "/shared").Body.String(); got != "from aaa" {
		t.Errorf("expected contested path to resolve to %q, got %q", "from aaa", got)
	}

	problems := strings.Join(s.GetConfigErrors(), "; ")
	if !strings.Contains(problems, `router "bbb"`) {
		t.Errorf("duplicate path not reported, got: %s", problems)
	}
	if !strings.Contains(problems, `router "nopath"`) {
		t.Errorf("empty path not reported, got: %s", problems)
	}

	// The watcher must still be alive and applying updates.
	s.GetConfigurationChan() <- routerConfig("/after", "still running", "v2")
	waitFor(t, 2*time.Second, "watcher to apply a later configuration", func() bool {
		return get(ep, "/after").Code == http.StatusOK
	})

	if errs := s.GetConfigErrors(); len(errs) != 0 {
		t.Errorf("expected clean config to clear errors, got %v", errs)
	}
}

// TestBuildHandlerChainIsDeterministic guards the sorted iteration order. Go
// randomises map iteration, so without sorting the winner of a contested path
// would vary between builds of the same configuration.
func TestBuildHandlerChainIsDeterministic(t *testing.T) {
	s := NewServer()
	config := Configuration{
		Routers: map[string]RouterConfig{
			"aaa": {Path: "/shared", ResponseText: "from aaa"},
			"bbb": {Path: "/shared", ResponseText: "from bbb"},
			"ccc": {Path: "/shared", ResponseText: "from ccc"},
		},
	}

	for i := 0; i < 50; i++ {
		handler, problems := s.buildHandlerChain(config)

		req := httptest.NewRequest(http.MethodGet, "/shared", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if got := rec.Body.String(); got != "from aaa" {
			t.Fatalf("build %d: expected %q, got %q", i, "from aaa", got)
		}
		if len(problems) != 2 {
			t.Fatalf("build %d: expected 2 skipped routers, got %d (%v)", i, len(problems), problems)
		}
	}
}
