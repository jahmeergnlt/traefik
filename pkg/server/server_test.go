package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestConcurrentConfigurationUpdates verifies that under concurrent provider
// updates, each request sees a consistent handler+middleware pair — i.e. the
// response body and header always match the same configuration version.
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
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
						"mw2": {HeaderName: "X-Test-Header", HeaderValue: "New"},
					},
				}
				s.GetConfigurationChan() <- configA
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Provider B: updates an unrelated router, keeps route1 using mw1 (Old)
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
						"mw1": {HeaderName: "X-Test-Header", HeaderValue: "Old"},
					},
				}
				s.GetConfigurationChan() <- configB
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Continuous HTTP request stream — verify body/header consistency
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

// TestConfigHandlerConsistency verifies that GetConfig() and the active handler
// are always in sync — GetConfig never reports a configuration that the handler
// does not yet serve.
func TestConfigHandlerConsistency(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	ep := s.GetEntryPoint("web")

	for i := 0; i < 100; i++ {
		cfg := Configuration{
			Routers: map[string]RouterConfig{
				"r": {Path: "/api", Middleware: "m", ResponseText: "v" + string(rune('0'+i%10))},
			},
			Middlewares: map[string]MiddlewareConfig{
				"m": {HeaderName: "X-V", HeaderValue: "v" + string(rune('0'+i%10))},
			},
		}
		s.GetConfigurationChan() <- cfg
		time.Sleep(1 * time.Millisecond)

		// Send a request and check that the response body matches
		// the config that GetConfig reports.
		req := httptest.NewRequest(http.MethodGet, "/api", nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)

		gotConfig := s.GetConfig()
		if rec.Code == http.StatusOK {
			expectedBody := gotConfig.Routers["r"].ResponseText
			if rec.Body.String() != expectedBody {
				t.Errorf("Config/handler mismatch: handler returned %q but config has %q",
					rec.Body.String(), expectedBody)
			}
		}
	}
}

// TestNoAtomicTypePanic verifies that rapid configuration updates never
// cause an atomic.Value type-panic.
func TestNoAtomicTypePanic(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Send many different configuration shapes rapidly
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			cfg := Configuration{
				Routers: map[string]RouterConfig{
					"a": {Path: "/a", Middleware: "", ResponseText: "hello"},
					"b": {Path: "/b", Middleware: "mw", ResponseText: "world"},
				},
				Middlewares: map[string]MiddlewareConfig{
					"mw": {HeaderName: "X-H", HeaderValue: "ok"},
				},
			}
			s.GetConfigurationChan() <- cfg

			cfg2 := Configuration{
				Routers:     map[string]RouterConfig{},
				Middlewares: map[string]MiddlewareConfig{},
			}
			s.GetConfigurationChan() <- cfg2
		}
		close(done)
	}()

	<-done
	time.Sleep(50 * time.Millisecond)

	// If we get here without panic, the test passes.
	ep := s.GetEntryPoint("web")
	req := httptest.NewRequest(http.MethodGet, "/a", nil)
	rec := httptest.NewRecorder()
	ep.ServeHTTP(rec, req)
	// Just ensure we can still serve requests
	if rec.Code != http.StatusOK && rec.Code != http.StatusNotFound {
		t.Errorf("unexpected status after rapid updates: %d", rec.Code)
	}
}

// TestMultipleEntryPoints verifies consistent behavior with multiple entrypoints.
func TestMultipleEntryPoints(t *testing.T) {
	s := NewServer()
	// Add a second entrypoint manually
	s.mu.Lock()
	ep2 := &EntryPoint{}
	ep2.state.Store(&serverState{
		config: Configuration{},
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}),
	})
	s.entryPoints["web2"] = ep2
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	cfg := Configuration{
		Routers: map[string]RouterConfig{
			"r1": {Path: "/", Middleware: "", ResponseText: "ok"},
		},
		Middlewares: map[string]MiddlewareConfig{},
	}
	s.GetConfigurationChan() <- cfg
	time.Sleep(30 * time.Millisecond)

	// Both entrypoints should still work after config update
	for _, name := range []string{"web", "web2"} {
		ep := s.GetEntryPoint(name)
		if ep == nil {
			t.Fatalf("entrypoint %q is nil", name)
		}
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		ep.ServeHTTP(rec, req)
	}
}

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

func TestDefaultHandler404BeforeConfig(t *testing.T) {
	s := NewServer()

	rec := get(s.GetEntryPoint("web"), "/anything")
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 before any config, got %d", rec.Code)
	}
}

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


// --- Additional comprehensive concurrent config swap tests ---

func TestConcurrentProviderUpdatesExt(t *testing.T) {
    srv, addr := startTestServer(t)
    defer srv.Close()
    const numWorkers = 20
    const numUpdates = 50
    var wg sync.WaitGroup
    for w := 0; w < numWorkers; w++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for i := 0; i < numUpdates; i++ {
                cfg := makeConfig(workerID*1000 + i)
                cfg.entryPoints = map[string]*EntryPoint{"http": {Address: addr}}
                srv.updateConfiguration(cfg)
            }
        }(w)
    }
    wg.Wait()
    resp, err := http.Get("http://" + addr + "/")
    if err == nil {
        resp.Body.Close()
    }
}

func TestConfigIntegrityDuringSwap(t *testing.T) {
    srv, addr := startTestServer(t)
    defer srv.Close()
    var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
        for i := 0; i < 100; i++ {
            cfg := srv.getConfig()
            if cfg == nil {
                t.Error("nil config during swap")
                return
            }
            time.Sleep(time.Millisecond)
        }
    }()
    go func() {
        defer wg.Done()
        for i := 0; i < 100; i++ {
            cfg := makeConfig(i)
            cfg.entryPoints = map[string]*EntryPoint{"http": {Address: addr}}
            srv.updateConfiguration(cfg)
            time.Sleep(time.Microsecond * 100)
        }
    }()
    wg.Wait()
}

func TestMultipleProviderSimultaneousUpdates(t *testing.T) {
    srv, addr := startTestServer(t)
    defer srv.Close()
    providers := []string{"file", "kubernetes", "consul", "etcd", "redis"}
    var wg sync.WaitGroup
    for _, providerName := range providers {
        wg.Add(1)
        go func(p string) {
            defer wg.Done()
            for i := 0; i < 30; i++ {
                cfg := makeConfig(i)
                cfg.entryPoints = map[string]*EntryPoint{"http": {Address: addr}}
                srv.updateConfiguration(cfg)
                time.Sleep(time.Microsecond * 200)
            }
        }(providerName)
    }
    wg.Wait()
    resp, err := http.Get("http://" + addr + "/")
    if err != nil {
        t.Fatal("server not responding:", err)
    }
    resp.Body.Close()
}
