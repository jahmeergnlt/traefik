package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// configForGeneration builds a Configuration in which route1's response body
// and its middleware header value carry the same generation marker. A torn
// update (new router config + stale middleware chain, or vice versa) is
// therefore observable as a body/header marker mismatch.
func configForGeneration(gen int) Configuration {
	marker := "gen-" + strconv.Itoa(gen)
	mw := "mw-" + strconv.Itoa(gen)
	return Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:         "/test",
				Middleware:   mw,
				ResponseText: marker,
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			mw: {
				HeaderName:  "X-Gen-Header",
				HeaderValue: marker,
			},
		},
	}
}

// TestConcurrentProvidersNeverServeTornState simulates multiple providers
// blasting conflicting updates while clients stream requests. Every observed
// response must pair a router body with the middleware header produced by the
// same configuration generation — never a stale/mismatched chain.
func TestConcurrentProvidersNeverServeTornState(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	const providers = 4
	const generationsPerProvider = 30

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Providers each emit a rapid sequence of distinct generations.
	for p := 0; p < providers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for g := 0; g < generationsPerProvider; g++ {
				select {
				case <-stop:
					return
				case s.GetConfigurationChan() <- configForGeneration(p*generationsPerProvider + g):
				}
			}
		}(p)
	}

	// Clients continuously hit the router and assert the body/header pair is
	// never torn across generations.
	const clients = 4
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := s.GetEntryPoint("web")
			for i := 0; i < 500; i++ {
				rec := httptest.NewRecorder()
				ep.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
				if rec.Code != http.StatusOK {
					continue // pre-config or drained state
				}
				body := rec.Body.String()
				hdr := rec.Header().Get("X-Gen-Header")
				if body != hdr {
					t.Errorf("torn state: body %q served with middleware header %q", body, hdr)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Final state check: the configuration reported via GetConfig must be
	// exactly what the active handler chain serves.
	cfg := s.GetConfig()
	rc, ok := cfg.Routers["route1"]
	if !ok {
		t.Fatal("route1 missing from reported configuration")
	}
	mw, ok := cfg.Middlewares[rc.Middleware]
	if !ok {
		t.Fatalf("middleware %q missing from reported configuration", rc.Middleware)
	}

	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
	if rec.Body.String() != rc.ResponseText {
		t.Errorf("handler body %q does not match reported router config %q", rec.Body.String(), rc.ResponseText)
	}
	if got := rec.Header().Get(mw.HeaderName); got != mw.HeaderValue {
		t.Errorf("handler header %q does not match reported middleware config %q", got, mw.HeaderValue)
	}
}

// TestProviderMutationAfterSend verifies that a provider mutating its maps
// after emitting an update cannot corrupt the active configuration or the
// live handler chain.
func TestProviderMutationAfterSend(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	config := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/test", Middleware: "mw1", ResponseText: "Stable"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Stable"},
		},
	}
	s.GetConfigurationChan() <- config

	// Wait until the update has actually been applied. Reading the applied
	// state back through GetConfig synchronizes with the atomic store in
	// switchConfigs, guaranteeing the server's copy has finished before the
	// provider mutates its own maps below.
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := s.GetConfig()
		if got.Routers["route1"].ResponseText == "Stable" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("configuration was not applied in time")
		}
		time.Sleep(time.Millisecond)
	}

	// Provider reuses and mutates the same maps after the send.
	config.Routers["route1"] = RouterConfig{Path: "/evil", ResponseText: "Corrupted"}
	config.Middlewares["mw1"] = MiddlewareConfig{HeaderName: "X-Evil", HeaderValue: "Corrupted"}
	delete(config.Routers, "route1")

	time.Sleep(20 * time.Millisecond)

	got := s.GetConfig()
	if got.Routers["route1"].ResponseText != "Stable" {
		t.Fatalf("provider mutation corrupted stored config: %+v", got.Routers["route1"])
	}
	if got.Middlewares["mw1"].HeaderValue != "Stable" {
		t.Fatalf("provider mutation corrupted stored middleware: %+v", got.Middlewares["mw1"])
	}

	rec := httptest.NewRecorder()
	s.GetEntryPoint("web").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))
	if rec.Body.String() != "Stable" || rec.Header().Get("X-Test-Header") != "Stable" {
		t.Fatalf("provider mutation corrupted live handler: body=%q header=%q",
			rec.Body.String(), rec.Header().Get("X-Test-Header"))
	}
}

// TestGetConfigReturnsIndependentCopy ensures callers cannot mutate the live
// configuration through the maps returned by GetConfig.
func TestGetConfigReturnsIndependentCopy(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.GetConfigurationChan() <- Configuration{
		Routers: map[string]RouterConfig{
			"route1": {Path: "/test", Middleware: "mw1", ResponseText: "Stable"},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Stable"},
		},
	}

	// Wait until the update is applied before inspecting it.
	deadline := time.Now().Add(2 * time.Second)
	var got Configuration
	for {
		got = s.GetConfig()
		if got.Routers["route1"].ResponseText == "Stable" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("configuration was not applied in time")
		}
		time.Sleep(time.Millisecond)
	}
	got.Routers["route1"] = RouterConfig{Path: "/evil", ResponseText: "Corrupted"}
	got.Middlewares["mw1"] = MiddlewareConfig{HeaderName: "X-Evil", HeaderValue: "Corrupted"}
	delete(got.Routers, "route1")

	fresh := s.GetConfig()
	if fresh.Routers["route1"].ResponseText != "Stable" {
		t.Fatal("GetConfig exposes mutable live router state")
	}
	if fresh.Middlewares["mw1"].HeaderValue != "Stable" {
		t.Fatal("GetConfig exposes mutable live middleware state")
	}
}
