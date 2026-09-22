package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func benchmarkConfiguration(n int) Configuration {
	c := Configuration{Routers: make(map[string]RouterConfig, n), Middlewares: map[string]MiddlewareConfig{
		"mw": {HeaderName: "X-Test", HeaderValue: "v"},
	}}
	for i := 0; i < n; i++ {
		c.Routers[fmt.Sprint(i)] = RouterConfig{Path: fmt.Sprintf("/r%d", i), Middleware: "mw", ResponseText: "ok"}
	}
	return c
}

func BenchmarkReload(b *testing.B) {
	for _, n := range []int{1, 100} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := NewServer()
			c := benchmarkConfiguration(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.switchConfigs(c)
			}
		})
	}
}

// Discard the body without allocating a recorder for every request.
type benchmarkWriter struct{ header http.Header }

func (w *benchmarkWriter) Header() http.Header         { return w.header }
func (w *benchmarkWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *benchmarkWriter) WriteHeader(int)             {}

func BenchmarkRequest(b *testing.B) {
	s := NewServer()
	s.switchConfigs(benchmarkConfiguration(100))
	ep := s.GetEntryPoint("web")
	req := httptest.NewRequest(http.MethodGet, "/r0", nil)
	w := &benchmarkWriter{header: make(http.Header)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ep.ServeHTTP(w, req)
	}
}
